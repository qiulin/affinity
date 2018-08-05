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

import java.util.concurrent.atomic.AtomicReference

import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.core.cluster.Coordinator.MembershipUpdate
import io.amient.affinity.{AffinityActorSystem, Conf}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class CoordinatorEmbeddedSpec extends FlatSpec with Matchers {

  "CoordinatorEmbedded instances" should "share the underlying space for the same id and group" in {
    val config = ConfigFactory.empty()
      .withValue(Conf.Affi.SystemName.path, ConfigValueFactory.fromAnyRef("101"))
      .withValue(Conf.Affi.Node.path, ConfigValueFactory.fromMap(Map[String, String]().asJava))
      .withValue(Conf.Affi.Avro.Class.path, ConfigValueFactory.fromAnyRef(classOf[MemorySchemaRegistry].getName))
      .withValue(Conf.Affi.Coordinator.Class.path, ConfigValueFactory.fromAnyRef(classOf[CoordinatorEmbedded].getName))
    val system = AffinityActorSystem.create(config)
    try {
      val coordinator1 = Coordinator.create(system, "group1")
      val actor1 = system.actorOf(Props(new Actor {
        override def receive: Receive = {
          case null =>
        }
      }), "actor1")
      coordinator1.register(actor1.path)
      val update1 = new AtomicReference[scala.collection.Set[ActorRef]](Set.empty)
      update1 synchronized {
        coordinator1.watch(system.actorOf(Props(new Actor {
          override def receive: Receive = {
            case MembershipUpdate(masters) => update1 synchronized update1.set(masters.values.toSet)
          }
        }), "subscriber1"))
      }
      coordinator1.close()

      val coordinator2 = Coordinator.create(system, "group1")
      val update2 = new AtomicReference[scala.collection.Set[ActorRef]](Set.empty)
      update2 synchronized {
        coordinator2.watch(system.actorOf(Props(new Actor {
          override def receive: Receive = {
            case MembershipUpdate(masters) => update2 synchronized update2.set(masters.values.toSet)
          }
        }), "subscriber2"))
        update2.wait(1000)
        update2.get.map(_.path.toString) should be(Set("akka://101/user/actor1"))
        update1.get.map(_.path.toString) should be(Set("akka://101/user/actor1"))
      }
      coordinator2.close()


    } finally {
      system.terminate()
    }

  }

}
