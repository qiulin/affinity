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

package io.amient.affinity.core.cluster

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{ActorPath, ActorRef, ActorSystem}
import akka.event.Logging
import akka.util.Timeout
import com.typesafe.config.Config
import io.amient.affinity.Conf
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.Controller.FatalErrorShutdown
import io.amient.affinity.core.config.CfgStruct
import io.amient.affinity.core.util.{ByteUtils, Reply}

import scala.collection.JavaConverters._
import scala.collection.Set
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object Coordinator {

  object CoordinatorConf extends CoordinatorConf {
    override def apply(config: Config): CoordinatorConf = new CoordinatorConf().apply(config)
  }

  class CoordinatorConf extends CfgStruct[CoordinatorConf] {
    val Class = cls("class", classOf[Coordinator], classOf[CoordinatorZk])
      .doc("implementation of coordinator must extend cluster.Coordinator")

    override protected def specializations(): util.Set[String] = Set("zookeeper", "embedded").asJava
  }

  final case class MasterUpdates(add: Set[ActorRef], remove: Set[ActorRef]) extends Reply[Unit] {
    def localTo(actor: ActorRef): MasterUpdates = {
      MasterUpdates(
        add.filter(_.path.address == actor.path.address),
        remove.filter(_.path.address == actor.path.address)
      )
    }
  }

  def create(system: ActorSystem, group: String): Coordinator = {
    val config = system.settings.config
    val conf: CoordinatorConf =  Conf(config).Affi.Coordinator
    val constructor = conf.Class().getConstructor(classOf[ActorSystem], classOf[String], classOf[CoordinatorConf])
    constructor.newInstance(system, group, conf)
  }
}

/**
  * @param group coordinated group name
  */
abstract class Coordinator(val system: ActorSystem, val group: String) {

  import Coordinator._
  import system.dispatcher

  implicit val scheduler = system.scheduler

  private val logger = Logging.getLogger(system, this)

  private val handles = scala.collection.mutable.Map[String, ActorRef]()

  def members: Map[String, String] = handles.toMap.mapValues(_.path.toString)

  protected val closed = new AtomicBoolean(false)

  /**
    * wacthers - a list of all actors that will receive AddMaster and RemoveMaster messages
    * when there are changes in the cluster. The value is global flag - `true` means that the
    * watcher is interested for changes at the global/cluster level, `false` means that the
    * watcher is only intereseted in changes in the local system.
    */
  protected val watchers = scala.collection.mutable.Map[ActorRef, Boolean]()

  /**
    * @param actorPath of the actor that needs to managed as part of coordinated group
    * @return unique coordinator handle which points to the registered ActorPath
    */
  def register(actorPath: ActorPath): String

  /**
    * unregister previously registered ActorPath
    *
    * @param handle handler returned from the prior register() method call
    * @return
    */
  def unregister(handle: String): Unit

  /**
    * watch changes in the coordinate group of routees in the whole cluster.
    *
    * @param watcher     actor which will receive the messages
    * @param clusterWide if true, the watcher will be notified of master status changes in the entire cluster
    *                    if false, the watcher will be notified of master status changes local to that watcher
    */
  def watch(watcher: ActorRef, clusterWide: Boolean): Unit = {
    synchronized {
      watchers += watcher -> clusterWide

      val currentMasters = getCurrentMasters.filter(clusterWide || _.path.address.hasLocalScope)
      val update = MasterUpdates(currentMasters, Set())
      implicit val timeout = Timeout(30 seconds)
      val informed = watcher ?! (if (clusterWide) update else update.localTo(watcher))
      informed.failed.foreach {
        case e: Throwable => if (!closed.get) {
          system.eventStream.publish(
            FatalErrorShutdown(new RuntimeException(
              "Could not send initial master status to watcher. This is could lead to inconsistent view of the cluster, " +
                "terminating the system.", e)))
        }
      }
    }
  }

  def unwatch(watcher: ActorRef): Unit = {
    synchronized {
      watchers -= watcher
    }
  }

  def close(): Unit = {
    closed.set(true)
    synchronized {
      watchers.clear()
      handles.clear()
    }
  }

  def isClosed = closed.get

  final protected def updateGroup(newState: Map[String, String]): Unit = {
    if (!closed.get) {

      val attempts = Future.sequence(newState.map { case (handle, actorPath) =>
        val selection = system.actorSelection(actorPath)
        implicit val timeout = new Timeout(30 seconds)
        selection.resolveOne() map (a => Success((handle, a))) recover {
          case NonFatal(e) =>
            logger.warning(s"$handle: ${e.getMessage}")
            Failure(e)
        }
      })

      val actorRefs: Future[Iterable[(String, ActorRef)]] = attempts.map(_.collect {
        case Success((handle, actor)) => (handle, actor)
      })

      val actors = Await.result(actorRefs, 1 minute)

      synchronized {
        val prevMasters: Set[ActorRef] = getCurrentMasters
        handles.clear()

        actors.foreach { case (handle, actor) =>
          handles.put(handle, actor)
        }

        val currentMasters: Set[ActorRef] = getCurrentMasters

        val add = currentMasters.filter(!prevMasters.contains(_))
        val remove = prevMasters.filter(!currentMasters.contains(_))
        if (!add.isEmpty || !remove.isEmpty) {
          val update = MasterUpdates(add, remove)
          notifyWatchers(update)
        }
      }
    }

  }

  //this is the method that is deterministic across all nodes. it resolves which one of the
  //replicas is the master for each partition applying a murmur2 hash on the actor handle
  //selecting the actor with the smallest hash
  private def getCurrentMasters: Set[ActorRef] = {
    handles.map(_._2.path.toStringWithoutAddress).toSet[String].map { relPath =>
      handles.filter(_._2.path.toStringWithoutAddress == relPath).minBy(x => ByteUtils.murmur2(x._1.getBytes))._2
    }
  }


  private def notifyWatchers(fullUpdate: MasterUpdates) = {
    if (!closed.get) watchers.foreach { case (watcher, global) =>
      implicit val timeout = Timeout(30 seconds)
      try {
        val informed = watcher ?! (if (global) fullUpdate else fullUpdate.localTo(watcher))
        informed.failed.foreach {
          case e: Throwable => if (!closed.get) {
            logger.error(e, s"Could not notify watcher: $watcher(global = $global)")
          }
        }
      } catch {
        case e: Throwable => if (!closed.get) {
          logger.error(e, s"Could not notify watcher: $watcher(global = $global)")
        }
      }
    }
  }


}
