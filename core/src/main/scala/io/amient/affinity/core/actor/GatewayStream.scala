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

package io.amient.affinity.core.actor

import java.io.Closeable
import java.util.concurrent.{Executors, TimeUnit}

import io.amient.affinity.core.actor.Controller.FatalErrorShutdown
import io.amient.affinity.core.serde.{AbstractSerde, Serde}
import io.amient.affinity.core.storage.{LogStorage, LogStorageConf, Record}
import io.amient.affinity.core.util.{CompletedJavaFuture, EventTime, OutputDataStream, TimeRange}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.immutable.ParSeq
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}
import scala.language.{existentials, postfixOps}
import scala.reflect.ClassTag

trait GatewayStream extends Gateway {

  @volatile private var closed = false
  @volatile private var suspendedSync = true

  private val lock = new Object

  private val config = context.system.settings.config

  private val nodeConf = conf.Affi.Node

  type InputStreamProcessor[K, V] = Record[K, V] => Future[Any]

  private val declaredInputStreamProcessors = new mutable.ListBuffer[RunnableInputStream[_, _]]

  private val declardOutputStreams = new ListBuffer[OutputDataStream[_, _]]

  lazy val outputStreams: ParSeq[OutputDataStream[_, _]] = declardOutputStreams.result().par

  def output[K: ClassTag, V: ClassTag](streamIdentifier: String): OutputDataStream[K, V] = {
    val streamConf = nodeConf.Gateway.Stream(streamIdentifier)
    if (!streamConf.Class.isDefined) {
      logger.warning(s"Output stream is not enabled in the current configuration: $streamIdentifier")
      null
    } else {
      val keySerde: AbstractSerde[K] = Serde.of[K](config)
      val valSerde: AbstractSerde[V] = Serde.of[V](config)
      val outpuDataStream = new OutputDataStream[K, V](keySerde, valSerde, streamConf)
      declardOutputStreams += outpuDataStream
      outpuDataStream
    }
  }

  /**
    * Create an input stream handler which will be managed by the gateway by giving it a processor function
    *
    * @param streamIdentifier id of the stream configuration object
    * @param processor        a function that takes (Record[K,V]) and returns Boolean signal that informs the committer
    * @tparam K
    * @tparam V
    */
  def input[K: ClassTag, V: ClassTag](streamIdentifier: String)(processor: InputStreamProcessor[K, V]): Unit = {
    val streamConf = nodeConf.Gateway.Stream(streamIdentifier)
    if (!streamConf.Class.isDefined) {
      logger.warning(s"Input stream is not enabled in the current configuration: $streamIdentifier")
    } else {
      val keySerde: AbstractSerde[K] = Serde.of[K](config)
      val valSerde: AbstractSerde[V] = Serde.of[V](config)
      declaredInputStreamProcessors += new RunnableInputStream[K, V](streamIdentifier, keySerde, valSerde, streamConf, processor)
    }
  }

  val inputStreamManager = new Thread {
    override def run(): Unit = {
      val inputStreamProcessors = declaredInputStreamProcessors.result()
      if (inputStreamProcessors.isEmpty) return
      val inputStreamExecutor = Executors.newFixedThreadPool(inputStreamProcessors.size)
      try {
        inputStreamProcessors.foreach(inputStreamExecutor.submit)
        while (!closed) {
          lock.synchronized(lock.wait(1000))
        }
        inputStreamExecutor.shutdown()
        inputStreamProcessors.foreach(_.close)
        inputStreamExecutor.awaitTermination(nodeConf.ShutdownTimeoutMs(), TimeUnit.MILLISECONDS)
      } finally {
        inputStreamExecutor.shutdownNow()
      }
    }
  }

  abstract override def preStart(): Unit = {
    inputStreamManager.start()
    super.preStart()
  }

  abstract override def postStop(): Unit = {
    try if (!closed) {
      lock.synchronized {
        closed = true
        lock.notifyAll()
      }
      logger.debug("Closing input streams")
      inputStreamManager.synchronized {
        inputStreamManager.join()
      }
      logger.debug("Closing output streams")
      outputStreams.foreach(_.close())
    } finally {
      super.postStop()
    }
  }

  abstract override def suspend() = try synchronized {
    lock.synchronized {
      this.suspendedSync = true
      lock.notifyAll()
    }
  } finally {
    super.suspend
  }

  abstract override def resume() = try synchronized {
    lock.synchronized {
      this.suspendedSync = false
      lock.notifyAll()
    }
  } finally {
    super.resume
  }

  class RunnableInputStream[K, V](identifier: String,
                                  keySerde: AbstractSerde[K],
                                  valSerde: AbstractSerde[V],
                                  streamConfig: LogStorageConf,
                                  processor: InputStreamProcessor[K, V]) extends Runnable with Closeable {

    val minTimestamp = streamConfig.MinTimestamp()
    val consumer = LogStorage.newInstanceEnsureExists(streamConfig)
    //this type of buffering has quite a high memory footprint but doesn't require a data structure with concurrent access
    val work = new ListBuffer[Future[Any]]
    val commitInterval: Long = streamConfig.CommitIntervalMs()
    val commitTimeout: Long = streamConfig.CommitTimeoutMs()

    override def close(): Unit = consumer.cancel()

    override def run(): Unit = {
      implicit val executor = scala.concurrent.ExecutionContext.Implicits.global
      var lastCommit: java.util.concurrent.Future[java.lang.Long] = new CompletedJavaFuture(0L)

      try {
        consumer.resume(TimeRange.since(minTimestamp))
        logger.info(s"Initializing input stream processor: $identifier, starting from: ${EventTime.local(minTimestamp)}, details: ${streamConfig}")
        var lastCommitTimestamp = System.currentTimeMillis()
        var finalized = false
        var uncommittedInput = false
        logger.info(s"Starting input stream processor: $identifier")
        while ((!closed && !finalized) || !lastCommit.isDone) {
          //clusterSuspended is volatile so we check it for each message set, in theory this should not matter because whatever the processor() does
          //should be suspended anyway and hang so no need to do it for every record
          if (suspendedSync) {
            logger.info(s"Pausing input stream processor: $identifier")
            while (suspendedSync) {
              lock.synchronized(lock.wait())
              if (closed) return
            }
            logger.info(s"Resuming input stream processor: $identifier")
          }
          val entries = consumer.fetch(true)
          if (entries != null) for (entry <- entries.asScala) {
            val key: K = keySerde.fromBytes(entry.key)
            val value: V = valSerde.fromBytes(entry.value)
            val unitOfWork = processor(new Record(key, value, entry.timestamp))
            if (!unitOfWork.isCompleted) work += unitOfWork
            uncommittedInput = true
          }
          /*  At-least-once guarantee processing input messages
           *  Every <commitInterval> all work is completed and then consumer is commited()
           */
          val now = System.currentTimeMillis()
          if ((closed && !finalized) || now - lastCommitTimestamp > commitInterval) try {
            //flush all pending work accumulated in this processor only
            val uncommittedWork = work.result
            work.clear
            if (uncommittedInput) {
              if (uncommittedWork.size > 0) {
                //await uncommitted work completion
                Await.result(Future.sequence(uncommittedWork), commitTimeout millis)
                //commit the records processed by this processor only since the last commit
              }
              lastCommit = consumer.commit() //trigger new commit
              //clear the uncommittedInput accumulator for the next commit
              uncommittedInput = false
            }
            lastCommitTimestamp = now
            if (closed) finalized = true
          } catch {
            case _: TimeoutException =>
              throw new TimeoutException(s"Input stream processor $identifier commit timed-out, consider increasing ${streamConfig.CommitTimeoutMs.path}")
          }
        }

      } catch {
        case _: InterruptedException =>
        case e: Throwable => context.system.eventStream.publish(FatalErrorShutdown(e))
      } finally {
        logger.info(s"Finished input stream processor: $identifier (closed = $closed)")
        consumer.close()
        keySerde.close()
        valSerde.close()
      }
    }


  }


}

