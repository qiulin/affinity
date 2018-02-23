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

package io.amient.affinity.spark

import java.time.{Duration, Instant}

import io.amient.affinity.Conf
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.avro.MemorySchemaRegistry.MemAvroConf
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.avro.record.{AvroRecord, AvroSerde}
import io.amient.affinity.core.actor.Routed
import io.amient.affinity.core.storage.{LogStorage, LogStorageConf}
import io.amient.affinity.core.util.{EventTime, OutputDataStream, TimeRange}
import io.amient.affinity.kafka.KafkaStorage.KafkaStorageConf
import io.amient.affinity.kafka.{EmbeddedKafka, KafkaLogStorage, KafkaStorage}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

case class CompactionTestEvent(key: Int, data: String, ts: Long) extends AvroRecord with EventTime with Routed {
  override def eventTimeUnix() = ts
}

object CompactRDDSpecUniverse {

  val topic = "test-topic"
  val DecemberFirst2017 = Instant.ofEpochMilli(1512086401000L)
  val JanuaryFirst2018 = Instant.ofEpochMilli(1514764801000L)
  val FebruaryFirst2018 = Instant.ofEpochMilli(1517443201000L)
  val schemaRegistryId = "345"

  def getSerdeConf = AvroConf(Map(
    AvroConf.Class.path -> classOf[MemorySchemaRegistry].getName,
    MemAvroConf(AvroConf).ID.path -> schemaRegistryId
  ))

  def getStorageConf(kafkaBootstrap: String) = new LogStorageConf().apply(Map(
    LogStorage.StorageConf.Class.path -> classOf[KafkaLogStorage].getName,
    KafkaStorageConf.BootstrapServers.path -> kafkaBootstrap,
    KafkaStorageConf.Topic.path -> topic
  ))

  def avroCompactRdd[K: ClassTag, V: ClassTag](avroConf: AvroConf, storageConf: LogStorageConf, range: TimeRange = TimeRange.UNBOUNDED)
                                              (implicit sc: SparkContext): RDD[(K, V)] = {
    CompactRDD(AvroSerde.create(avroConf), LogStorage.newInstance(storageConf), range)
  }

}


class CompactRDDSpec extends FlatSpec with EmbeddedKafka with Matchers with BeforeAndAfterAll {

  override def numPartitions = 10

  import CompactRDDSpecUniverse._

  implicit val sc = new SparkContext(new SparkConf()
    .setMaster("local[10]")
    .set("spark.driver.host", "localhost")
    .setAppName("Affinity_Spark_Test")
    .set("spark.serializer", classOf[KryoSerializer].getName)
    .set("spark.kryo.classesToRegister", "io.amient.affinity.spark.CompactionTestEvent"))


  override def beforeAll() {
    super.beforeAll()

    val stream = new OutputDataStream[Int, CompactionTestEvent](
      AvroSerde.create(getSerdeConf), AvroSerde.create(getSerdeConf), getStorageConf(kafkaBootstrap))
    try {
      stream.write((0 to 99).iterator.map { i =>
        (i, CompactionTestEvent(i, s"January($i)", JanuaryFirst2018.toEpochMilli + i * 1000))
      })

      stream.write((0 to 99).iterator.map { i =>
        (i, CompactionTestEvent(i, s"February($i)", FebruaryFirst2018.toEpochMilli + i * 1000))
      })

      stream.write((0 to 99).iterator.map { i =>
        (i, CompactionTestEvent(i, s"December($i)", DecemberFirst2017.toEpochMilli + i * 1000))
      })

    } finally {
      stream.close
    }
  }

  "full reset RDD" should "return fully compacted stream" in {
    val rdd = avroCompactRdd[Int, CompactionTestEvent](getSerdeConf, getStorageConf(kafkaBootstrap))
    val result = rdd.collect.sortBy(_._1)
    result.size should be (100)
    result.forall(_._2.eventTimeUnix >= FebruaryFirst2018.toEpochMilli)
  }

  "range reset RDD" should "return compacted range of the stream" in {
    val rdd = avroCompactRdd[Int, CompactionTestEvent](
      getSerdeConf, getStorageConf(kafkaBootstrap), TimeRange.prev(Duration.ofSeconds(50), Instant.from(Duration.ofSeconds(100).addTo(FebruaryFirst2018))))
    val result = rdd.collect.sortBy(_._1)
    result.size should be (50)
    result.forall(_._2.eventTimeUnix >= FebruaryFirst2018.toEpochMilli)
  }

}
