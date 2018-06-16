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
import java.util.{Observable, Observer}

import akka.actor.{Actor, ActorRef, Status}
import akka.event.Logging
import io.amient.affinity.Conf
import io.amient.affinity.core.actor.Container.{PartitionOffline, PartitionOnline}
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.util.Reply

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

//FIXME #122 make special serializer for mediation messages - currently this is java and the footprint is huge
case class CreateKeyValueMediator(stateStore: String, key: Any) extends Routed
case class KeyValueMediatorCreated(mediator: ActorRef)
case class RegisterMediatorSubscriber(subscriber: ActorRef)

case class BecomeStandby() extends Reply[Unit]
case class BecomeMaster() extends Reply[Unit]

trait Partition extends ActorHandler {

  private val log = Logging.getLogger(context.system, this)

  /**
    * keyspace identifier
    */
  implicit val keyspace = self.path.parent.name

  /**
    * partition id
    */
  implicit val partition = self.path.name.toInt

  private var started = false
  private val declaredStateStores: CopyOnWriteArrayList[(String, State[_, _])] = new CopyOnWriteArrayList[(String, State[_, _])]()
  private lazy val stateStores: Map[String, State[_, _]] = declaredStateStores.iterator().asScala.toMap

  def state[K: ClassTag, V: ClassTag](store: String)(implicit keyspace: String, partition: Int): State[K, V] = {
    if (started) throw new IllegalStateException("Cannot declare state after the actor has started")
    val conf = Conf(context.system.settings.config)
    val numPartitions = conf.Affi.Keyspace(keyspace).NumPartitions()
    val stateConf = conf.Affi.Keyspace(keyspace).State(store)
    val state = State.create[K, V](s"$keyspace-$store", partition, stateConf, numPartitions, context.system)
    declaredStateStores.add((store, state))
    state
  }


  override def preStart(): Unit = {
    started = true
    log.debug(s"Starting keyspace: $keyspace, partition: $partition")
    //on start-up every partition is a standby which also requires a blocking bootstrap first
    become(standby = true)
    //only after states have caught up we make the partition online and available to cooridnators
    context.parent ! PartitionOnline(self)
    super.preStart()
  }

  /**
    * onBecomeMaster will be fired when this Partition Actor has successfully become the Master for the
    * underlying physical partition and after all state stores have been caught-up and are at this
    * point consistent with the storage.
    */
  protected def onBecomeMaster: Unit = ()

  /**
    * onBecomeStandby will be fired when this Partition Actor became a standby for the underlying
    * physical partition and after all state stores have been switched to the background passive mode.
    */
  protected def onBecomeStandby: Unit = ()

  override def postStop(): Unit = {
    try {
      log.debug(s"Stopping keyspace: $keyspace, partition: $partition")
      context.parent ! PartitionOffline(self)
      closeStateStores()
    } finally super.postStop()
  }

  abstract override def manage: Receive = super.manage orElse {

    case msg@BecomeMaster() =>
      msg(sender) ! {} //acking the receipt of the instruction immediately
      become(standby = false) //then blocking the inbox until state stores have caught-up with storage
      onBecomeMaster //then invoke custom handler

    case msg@BecomeStandby() =>
      msg(sender) ! {} //acking the receipt of the instruction immediately
      become(standby = true)  //then switch state stores to standby mode, i.e. tailing the storage in the background
      onBecomeStandby

    case CreateKeyValueMediator(stateStoreName: String, key: Any) => try {
      val state = getStateStore(stateStoreName)
      val props = state.uncheckedMediator(self, key)
      sender ! KeyValueMediatorCreated(context.actorOf(props))
    } catch {
      case NonFatal(e) => sender ! Status.Failure(e)
    }
  }

  private[core] def getStateStore(stateStoreName: String): State[_, _] = {
    if (!started) throw new IllegalStateException("Cannot get state store rererence before the actor has started")
    stateStores(stateStoreName)
  }

  private[core] def become(standby: Boolean): Unit = {
    stateStores.values.foreach { state =>
      state.boot
      if (state.external || standby) state.tail
    }
    if (standby) {
      log.debug(s"Became standby for partition $keyspace/$partition")
    } else {
      log.debug(s"Became master for partition $keyspace/$partition")
    }
  }

  private[core] def closeStateStores(): Unit = stateStores.foreach {
    case (id, state) => try state.close catch {
      case NonFatal(e) => log.error(e, s"Could not close store $id")
    }
  }

}

class KeyValueMediator[K](partition: ActorRef, state: State[K, _], key: K) extends Actor {

  private var observer: Option[Observer] = None

  implicit val scheduler = context.system.scheduler

  override def postStop(): Unit = {
    observer.foreach(state.removeKeyValueObserver(key, _))
  }

  override def receive: Receive = {
    case RegisterMediatorSubscriber(subscriber:ActorRef) => createKeyValueObserver(key, subscriber)
    case forward: Any => partition.tell(forward, sender)
  }

  def createKeyValueObserver(key: K, frontend: ActorRef): Unit = {

    observer = Some(new Observer {
      override def update(o: Observable, arg: scala.Any): Unit = {
        frontend ! arg
      }
    })
    val observable = state.addKeyValueObserver(key, observer.get)

    // send initial value on subscription TODO - maybe this is up to the client, e.g. websocket impl., to decide
    observer.foreach(_.update(observable, state(key)))
  }


}

