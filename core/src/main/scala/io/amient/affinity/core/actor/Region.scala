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

import akka.actor.Props
import akka.event.Logging
import io.amient.affinity.core.ack._
import io.amient.affinity.core.actor.Service.{BecomeMaster, BecomeStandby}
import io.amient.affinity.core.cluster.Coordinator
import io.amient.affinity.core.cluster.Coordinator.MasterStatusUpdate

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object Region {
  final val CONFIG_PARTITION_LIST = "affinity.node.region.partitions"
}

class Region(coordinator: Coordinator, partitionProps: Props)
  extends Container(coordinator: Coordinator, "regions") {

  private val config = context.system.settings.config

  private val log = Logging.getLogger(context.system, this)

  import Region._

  val partitions = config.getIntList(CONFIG_PARTITION_LIST).asScala

  override def preStart(): Unit = {
    log.info("Starting Region")
    //FIXME coordinator.watch(self, global = false)
    for (partition <- partitions) {
      /**
        * partition actor name is the physical partition id which is relied upon by DeterministicRoutingLogic
        * as well as Partition
        */
      context.actorOf(partitionProps, name = partition.toString)
    }
    super.preStart()
  }

  override def postStop(): Unit = {
    coordinator.unwatch(self)
    super.postStop()
  }

  import context.dispatcher

  override def receive: Receive = super.receive orElse {

    case MasterStatusUpdate("regions", add, remove) => ack(sender) {}
      //TODO arbitrary ack timeouts
      Await.ready(Future.sequence(add.toList.map(ref => ack(ref, BecomeMaster()))), 1 hour)
      Await.ready(Future.sequence(remove.toList.map(ref => ack(ref, BecomeStandby()))), 1 minute)
    //}
  }

}
