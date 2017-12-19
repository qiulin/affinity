/*
 * Copyright 2016 Michal Harish, michal.harish@gmail.com
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

import java.util.concurrent.CopyOnWriteArrayList

import akka.actor.Actor
import akka.event.Logging
import io.amient.affinity.core.storage.{State, StateConf}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

trait ActorState extends Actor {

  private val log = Logging.getLogger(context.system, this)

  private val storageRegistry = new CopyOnWriteArrayList[(String, State[_, _])]()

  abstract override def postStop(): Unit = {
    super.postStop()
    closeState()
  }

  def state[K: ClassTag, V: ClassTag](store: String, conf: StateConf): State[K, V] = state[K, V](store, {
    conf.Name.setValue(store)
    State.create[K, V](conf, context.system)
  })

  def state[K: ClassTag, V: ClassTag](store: String)(implicit keyspace: String, partition: Int): State[K, V] = {
    state[K, V](store, {
      State.create[K, V](keyspace, partition, store, context.system)
    })
  }

  def state[K, V](name:String, creator: => State[K, V]): State[K, V] = {
    val result: State[K, V] = creator
    result.storage.init()
    result.storage.boot()
    result.storage.tail()
    storageRegistry.add((name, result))
    result
  }

  def getStateStore(stateStoreName: String): State[_, _] = {
    storageRegistry.asScala.find(_._1 == stateStoreName).get._2
  }

  def bootState(): Unit = storageRegistry.asScala.foreach { case (name, s) =>
    log.info(s"state store: '${name}', partition: ${s.storage.partition} booted, estimated num. keys=${s.numKeys}")
    s.storage.boot()
  }


  def tailState(): Unit = storageRegistry.asScala.foreach(_._2.storage.tail())

  def closeState(): Unit = {
    storageRegistry.asScala.foreach { case (name, store) =>
      log.debug(s"Closing state store $name ")
      store.storage.close()
    }
    storageRegistry.clear()
  }

}
