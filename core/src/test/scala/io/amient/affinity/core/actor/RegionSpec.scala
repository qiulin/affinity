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

import java.util.Properties

import akka.actor.Actor.Receive
import akka.actor.{ActorPath, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import io.amient.affinity.core.TestCoordinator
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._


class RegionSpec() extends TestKit(ActorSystem("MySpec")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  val testPartition = Props(new Service {
    override def preStart(): Unit = {
      Thread.sleep(100)
      super.preStart()
    }
    override def receiveService: Receive = {
      case e: IllegalStateException => throw e
      case any =>
    }
  })

  "A Region Actor" must {
    "must keep Coordinator Updated during partition failure & restart scenario" in {
      val services = scala.collection.mutable.Set[String]()
      val coordinator = new TestCoordinator(system, services)
      val props = new Properties()
      val d = 1 second
      implicit val timeout = Timeout(d)

      //wait for a region of 4 partitions to be online
      props.put(Region.CONFIG_PARTITION_LIST, "0,1,2,3")

      system.actorOf(Props(new Region(props, coordinator, testPartition)), name = "region")
      awaitCond (services.size == 4)

      //first stop Partition explicitly - it shouldn't be restarted
      import system.dispatcher
      system.actorSelection(ActorPath.fromString(services.head)).resolveOne() onSuccess {
        case actorRef => system.stop(actorRef)
      }
      awaitCond(services.size == 3)

      //now simulate error in one of the partitions
      val partitionToFail = services.head
      system.actorSelection(ActorPath.fromString(partitionToFail)).resolveOne() onSuccess {
        case actorRef => actorRef ! new IllegalStateException
      }
      awaitCond(services.size == 2 && !services.contains(partitionToFail))
//      it had a failure, it should be restarted
      awaitCond(services.size == 3)

    }
  }

}
