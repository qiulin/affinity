/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.affinity.kafka

import java.util.Properties
import java.util.concurrent.{ExecutionException, Future, TimeUnit}
import java.{lang, util}

import com.typesafe.config.Config
import io.amient.affinity.core.config.{Cfg, CfgStruct}
import io.amient.affinity.core.state.StateConf
import io.amient.affinity.core.storage._
import io.amient.affinity.core.util.{EventTime, JavaPromise, MappedJavaFuture, TimeRange}
import io.amient.affinity.kafka.KafkaStorage.KafkaStorageConf
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, ConfigEntry, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{TopicExistsException, UnknownServerException, WakeupException}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.reflectiveCalls

object KafkaStorage {

  object StateConf extends KafkaStateConf {
    override def apply(config: Config): KafkaStateConf = new KafkaStateConf().apply(config)
  }

  class KafkaStateConf extends CfgStruct[KafkaStateConf](classOf[StateConf]) {
    val Storage = struct("storage", new KafkaStorageConf, false)
  }

  object KafkaStorageConf extends KafkaStorageConf {
    override def apply(config: Config): KafkaStorageConf = new KafkaStorageConf().apply(config)
  }

  class KafkaStorageConf extends CfgStruct[KafkaStorageConf](classOf[LogStorageConf]) {
    val Topic = string("kafka.topic", true).doc("kafka topic name")
    val Partitions = integer("kafka.partitions", false).doc("requird number of partitions ")
    val ReplicationFactor = integer("kafka.replication.factor", 1).doc("replication factor of the kafka topic")
    val BootstrapServers = string("kafka.bootstrap.servers", true).doc("kafka connection string used for consumer and/or producer")
    val Producer = struct("kafka.producer", new KafkaProducerConf, false).doc("any settings that the underlying version of kafka producer client supports")
    val Consumer = struct("kafka.consumer", new KafkaConsumerConf, false).doc("any settings that the underlying version of kafka consumer client supports")
  }

  class KafkaProducerConf extends CfgStruct[KafkaProducerConf](Cfg.Options.IGNORE_UNKNOWN)

  class KafkaConsumerConf extends CfgStruct[KafkaConsumerConf](Cfg.Options.IGNORE_UNKNOWN) {
    val GroupId = string("group.id", false).doc("kafka consumer group.id will be used if it backs an input stream, state stores manage partitions internally")
  }

}

class KafkaLogStorage(conf: LogStorageConf) extends LogStorage[java.lang.Long] with ConsumerRebalanceListener with Callback {

  private val log = LoggerFactory.getLogger(classOf[KafkaLogStorage])

  val kafkaStorageConf = KafkaStorageConf(conf)

  val topic = kafkaStorageConf.Topic()
  val keySubject: String = s"${topic}-key"
  val valueSubject: String = s"${topic}-value"

  private val producerConfig = new Properties() {
    put("retries", Int.MaxValue.toString)
    put("max.in.flight.requests.per.connection", "1")
    put("max.block.ms", Long.MaxValue.toString)
    if (kafkaStorageConf.Producer.isDefined) {
      val producerConfig = kafkaStorageConf.Producer.config()
      if (producerConfig != null) {
        if (producerConfig.hasPath("bootstrap.servers")) throw new IllegalArgumentException("bootstrap.servers cannot be overriden for KafkaStroage producer")
        if (producerConfig.hasPath("key.serializer")) throw new IllegalArgumentException("Binary kafka stream cannot use custom key.serializer")
        if (producerConfig.hasPath("value.serializer")) throw new IllegalArgumentException("Binary kafka stream cannot use custom value.serializer")
        if (producerConfig.hasPath("max.in.flight.requests.per.connection")) log.warn("Changing producer max.in.flight.requests.per.connection from recommended: 1")
        if (producerConfig.hasPath("max.block.ms")) log.warn("Changing producer max.block.ms from recommended: Long.MaxValue")
        if (producerConfig.hasPath("retries")) log.warn("Changing producer retries from recommended: Int.MaxValue")
        producerConfig.entrySet.asScala.foreach { case (entry) =>
          put(entry.getKey, entry.getValue.unwrapped())
        }
      }
    }
    put("bootstrap.servers", kafkaStorageConf.BootstrapServers())
    put("value.serializer", classOf[ByteArraySerializer].getName)
    put("key.serializer", classOf[ByteArraySerializer].getName)
  }

  private val consumerProps = new Properties() {
    put("auto.offset.reset", "earliest")
    if (kafkaStorageConf.Consumer.isDefined) {
      val consumerConfig = kafkaStorageConf.Consumer.config()
      if (consumerConfig != null) {
        if (consumerConfig.hasPath("bootstrap.servers")) throw new IllegalArgumentException("bootstrap.servers cannot be overriden for KafkaStroage consumer")
        if (consumerConfig.hasPath("enable.auto.commit")) throw new IllegalArgumentException("enable.auto.commit cannot be overriden for KafkaStroage consumer")
        if (consumerConfig.hasPath("key.deserializer")) throw new IllegalArgumentException("key.deserializer cannot be overriden for KafkaStroage consumer")
        if (consumerConfig.hasPath("value.deserializer")) throw new IllegalArgumentException("value.deserializer cannot be overriden for KafkaStroage consumer")
        consumerConfig.entrySet.asScala.foreach { case (entry) =>
          put(entry.getKey, entry.getValue.unwrapped())
        }
      }
    }
    put("bootstrap.servers", kafkaStorageConf.BootstrapServers())
    put("enable.auto.commit", "false")
    put("key.deserializer", classOf[ByteArrayDeserializer].getName)
    put("value.deserializer", classOf[ByteArrayDeserializer].getName)
  }

  private val kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
  private val stopOffsets = new mutable.HashMap[Int, java.lang.Long]()
  private var closed = false
  private var range: TimeRange = TimeRange.UNBOUNDED

  override def getNumPartitions(): Int = {
    kafkaConsumer.partitionsFor(topic).size()
  }

  override def reset(partition: Int, range: TimeRange): Unit = {
    log.debug(s"Resetting $topic/$partition to $range")
    val tp = new TopicPartition(topic, partition)
    this.range = range
    kafkaConsumer.assign(List(tp).asJava)
    val beginOffset: Long = kafkaConsumer.beginningOffsets(List(tp).asJava).get(tp)
    val startOffset: Long = Option(kafkaConsumer.offsetsForTimes(Map(tp -> new java.lang.Long(range.start)).asJava).get(tp)).map(_.offset).getOrElse(beginOffset)
    log.debug(s"Reset partition=${tp.partition()} time range ${range.getLocalStart}:${range.getLocalEnd}")
    reset(tp.partition, startOffset)
  }

  override def reset(partition: Int, startPosition: java.lang.Long): java.lang.Long = {
    val tp = new TopicPartition(topic, partition)
    val startOffset = if (startPosition == null || startPosition < 0) kafkaConsumer.beginningOffsets(List(tp).asJava).get(tp) else startPosition
    if (startOffset < 0) {
      return null
    } else {
      kafkaConsumer.seek(tp, startOffset)
      val maxOffset: Long = kafkaConsumer.endOffsets(List(tp).asJava).get(tp) - 1
      val stopOffset = maxOffset
      /* kafka at the moment supports only offsets-after(t), so if one day there is offsets-before(t) we can optimize more:
      val stopOffset: Long = Option(kafkaConsumer.offsetsBefore(Map(tp -> new java.lang.Long(range.end))).get(tp)).map(_.offset).getOrElse(maxOffset)
      */
      if (stopOffset >= startOffset) {
        stopOffsets.put(tp.partition, stopOffset)
      } else {
        stopOffsets.remove(tp.partition)
      }
      stopOffset
    }
  }

  override def resume(range: TimeRange): Unit = {
    this.range = range
    kafkaConsumer.subscribe(List(topic).asJava, this)
  }

  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]) = {
    partitions.asScala.foreach(tp => stopOffsets.remove(tp.partition))
  }

  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]) = {
    partitions.asScala.foreach {
      tp =>
        val beginOffset: Long = kafkaConsumer.beginningOffsets(List(tp).asJava).get(tp)
        val rangeStartOffset: Long = Option(kafkaConsumer.offsetsForTimes(Map(tp -> new java.lang.Long(range.start)).asJava).get(tp)).map(_.offset).getOrElse(beginOffset)
        val minOffset: Long = math.max(beginOffset, rangeStartOffset)
        val nextOffset: Long = Option(kafkaConsumer.committed(tp)).map(_.offset() + 1).getOrElse(0)
        val resumeOffset: Long = math.max(minOffset, nextOffset)
        if (minOffset > nextOffset) {
          log.info(s"Resuming partition=${tp.partition()} time range ${range.getLocalStart}:${range.getLocalEnd}, advancing to offset: ${resumeOffset}")
          kafkaConsumer.seek(tp, resumeOffset)
        }
        stopOffsets.remove(tp.partition)
    }
  }

  override def fetch(unbounded: Boolean): util.Iterator[LogEntry[java.lang.Long]] = {

    if (!unbounded && stopOffsets.isEmpty) {
      return null
    }

    val kafkaRecords = try {
      kafkaConsumer.poll(500)
    } catch {
      case _: WakeupException => return null
    }

    kafkaRecords.iterator.asScala.filter { record =>
      if (unbounded) {
        record.timestamp >= range.start && record.timestamp <= range.end
      } else if (!stopOffsets.contains(record.partition)) {
        false
      } else {
        if (record.offset >= stopOffsets(record.partition)) stopOffsets.remove(record.partition)
        record.timestamp >= range.start && record.timestamp <= range.end
      }
    }.map {
      case r => new LogEntry(new java.lang.Long(r.offset), r.key, r.value, r.timestamp)
    }
  }.asJava

  def cancel(): Unit = kafkaConsumer.wakeup()

  def commit(): JavaPromise[lang.Long] = {
    val promise = new JavaPromise[java.lang.Long]
    kafkaConsumer.commitAsync(new OffsetCommitCallback {
      def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception) = {
        if (exception != null) promise.failure(exception) else promise.success(System.currentTimeMillis())
      }
    })
    promise
  }

  private var producerActive = false
  @volatile private var produceException: Throwable = null

  lazy protected val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfig)

  override def append(record: Record[Array[Byte], Array[Byte]]): java.util.concurrent.Future[java.lang.Long] = {
    producerActive = true
    val producerRecord: ProducerRecord[Array[Byte], Array[Byte]] = if (record.key == null) {
      new ProducerRecord(topic, null, record.timestamp, null, record.value)
    } else {
      new ProducerRecord(topic, null, record.timestamp, record.key, record.value)
    }
    //setting the callback because that is the only way how to ensure flush throws exception on error
    new MappedJavaFuture[RecordMetadata, java.lang.Long](producer.send(producerRecord, this)) {
      override def map(result: RecordMetadata): java.lang.Long = {
        result.offset()
      }
    }
  }

  override def delete(key: Array[Byte]): Future[java.lang.Long] = {
    //kafka uses null value as a delete tombstone
    append(new Record[Array[Byte], Array[Byte]](key, null, EventTime.unix));
  }

  override def onCompletion(metadata: RecordMetadata, exception: Exception) = {
    if (exception != null) {
      produceException = exception
    }
  }

  override def flush() = if (producerActive) {
    //kafka producer flush doesn't throw exception so we need to use our own exception var set by producer.send callback
    if (produceException != null) throw produceException
    producer.flush()
  }

  override def close(): Unit = if (!closed) {
    try kafkaConsumer.close() finally try if (producerActive) producer.close() finally closed = true
  }

  override def isTombstone(entry: LogEntry[lang.Long]) = entry.value == null

  override def ensureExists(): Unit = {
    if (kafkaStorageConf.Partitions.isDefined) {
      ensureCorrectConfiguration(-1, kafkaStorageConf.Partitions(), true)
    } else {
      throw new IllegalArgumentException(s"storage configuration for topic $topic must have partitions property set to a postivie integer")
    }
  }

  override def ensureCorrectConfiguration(ttlMs: Long, numPartitions: Int, readonly: Boolean): Unit = {
    val adminProps = new Properties() {
      put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStorageConf.BootstrapServers())
      //the following is here to pass the correct security settings - maybe only security.* and sasl.* settings could be filtered
      if (kafkaStorageConf.Consumer.isDefined) {
        val consumerConfig = kafkaStorageConf.Consumer.config()
        if (consumerConfig != null) {
          val allowedAdminConfigs = AdminClientConfig.configNames
          consumerConfig.entrySet.asScala.filter(c => allowedAdminConfigs.contains(c.getKey)).foreach {
            case (entry) => put(entry.getKey, entry.getValue.unwrapped())
          }
        }
      }
    }
    val admin = AdminClient.create(adminProps)
    try {
      val adminTimeoutMs: Long = 15000
      val compactionPolicy = (if (ttlMs > 0) "compact,delete" else "compact")
      val replicationFactor = kafkaStorageConf.ReplicationFactor().toShort
      val topicConfigs = Map(
        TopicConfig.CLEANUP_POLICY_CONFIG -> compactionPolicy,
        TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG -> "CreateTime",
        TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG -> (if (ttlMs > 0) ttlMs else Long.MaxValue).toString,
        TopicConfig.RETENTION_MS_CONFIG -> (if (ttlMs > 0) ttlMs else Long.MaxValue).toString,
        TopicConfig.RETENTION_BYTES_CONFIG -> "-1"
      )

      var exists: Option[Boolean] = None
      while (!exists.isDefined) {
        if (admin.listTopics().names().get(adminTimeoutMs, TimeUnit.MILLISECONDS).contains(topic)) {
          exists = Some(true)
        } else {
          val schemaTopicRequest = new NewTopic(topic, numPartitions, replicationFactor)
          try {
            admin.createTopics(List(schemaTopicRequest).asJava).all.get(adminTimeoutMs, TimeUnit.MILLISECONDS)
            log.info(s"Created topic $topic, num.partitions: $numPartitions, replication factor: $replicationFactor")
            exists = Some(false)
          } catch {
            case e: ExecutionException if e.getCause.isInstanceOf[TopicExistsException] => //continue
            case e: ExecutionException if e.getCause.isInstanceOf[UnknownServerException] && e.getCause.getMessage.contains("NodeExists")=>
              //continue- since kafka 1.1 TopicExistsException is not wrapped correctly by the AdminClient
          }
        }
        TimeUnit.MILLISECONDS.sleep(300)
      }

      if (exists.get) {
        log.debug(s"Checking that topic $topic has correct number of partitions: ${numPartitions}")
        val description = admin.describeTopics(List(topic).asJava).values.asScala.head._2.get(adminTimeoutMs, TimeUnit.MILLISECONDS)
        if (description.partitions().size() != numPartitions) {
          throw new IllegalStateException(s"Kafka topic $topic has ${description.partitions().size()} partitions, expecting: $numPartitions")
        }
        log.debug(s"Checking that topic $topic has correct replication factor: ${replicationFactor}")
        val actualReplFactor = description.partitions().get(0).replicas().size()
        if (actualReplFactor < replicationFactor) {
          throw new IllegalStateException(s"Kafka topic $topic has $actualReplFactor, expecting: $replicationFactor")
        }
        log.debug(s"Checking that topic $topic contains all required configs: ${topicConfigs}")
        val topicConfigResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
        val actualConfig = admin.describeConfigs(List(topicConfigResource).asJava)
          .values().asScala.head._2.get(adminTimeoutMs, TimeUnit.MILLISECONDS)
        val configOutOfSync = topicConfigs.exists { case (k, v) => actualConfig.get(k).value() != v }
        if (readonly) {
          if (configOutOfSync) log.warn(s"External State configuration doesn't match the state configuration: $topicConfigs")
        } else if (configOutOfSync) {
          val entries = topicConfigs.map { case (k, v) => new ConfigEntry(k, v) }.asJavaCollection
          admin.alterConfigs(Map(topicConfigResource -> new org.apache.kafka.clients.admin.Config(entries)).asJava)
            .all().get(adminTimeoutMs, TimeUnit.MILLISECONDS)
          log.info(s"Topic $topic configuration altered successfully")
        } else {
          log.debug(s"Topic $topic configuration is up to date")
        }
      }
    } finally {
      admin.close()
    }
  }

}

