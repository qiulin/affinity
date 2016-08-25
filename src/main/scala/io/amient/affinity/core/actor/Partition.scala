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

import akka.actor.{Actor, Props, Status}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Partition {
  final case class SimulateError(e: Exception)

  sealed abstract class Keyed[K](key: K) extends Serializable {
    override def hashCode() = key.hashCode()
  }

  final case class ShowIndex(key: String) extends Keyed[String](key)
  final case class KillNode(partition: Int) extends Keyed[Int](partition)
  final case class CollectUserInput(greeting: String) extends Keyed(greeting)
  final case class StringEntry(key: String, value: String) extends Keyed[String](key)
  final case class GetStringEntry(key: String) extends Keyed[String](key)
}

import io.amient.affinity.core.actor.Partition._


class Partition(partition: Int) extends Actor {

  import context.dispatcher

  val log = Logging.getLogger(context.system, this)

  val id = self.path.toString

  var cache = Map.empty[String, String]

  val userInputMediator = context.actorOf(Props(new UserInputMediator))

  override def postStop(): Unit = {
    log.info("Committing Partition ChangeLog: " + self.path.name)
    //TODO commit change log
    //TODO clear state (if this is not a new instance)
    context.parent ! Region.PartitionOffline(self)
    super.postStop()
  }

  override def preStart(): Unit = {
    log.info("Bootstrapping partition: " + self.path.name)
    //TODO bootstrap data
    Thread.sleep(1500)
    context.parent ! Region.PartitionOnline(self)
  }


  def receive = {

    case SimulateError(e) =>
      log.warning("TestError instruction in partition " + context.system.name)
      throw e

    case KillNode(_) =>
      log.warning("killing the entire node " + context.system.name)
      implicit val timeout = Timeout(10 seconds)
      context.actorSelection("/user").resolveOne().onSuccess{
        case controller => context.stop(controller)
      }


    case ShowIndex(msg) =>
      sender ! s"${self.path.name}:$msg"

    case CollectUserInput(greeting) =>
      implicit val timeout = Timeout(60 seconds)
      val origin = sender()
      val prompt = s"${self.path.name}: $greeting >"
      userInputMediator ? prompt andThen {
        case Success(userInput: String) =>
          if (userInput == "error") origin ! Status.Failure(throw new RuntimeException(userInput))
          else origin ! userInput
        case Failure(e) => origin ! Status.Failure(e)
      }

    case StringEntry(key, value) =>
      val msg = s"${self.path.name}: PUT ($key, $value)"
      if (log.isDebugEnabled) {
        log.debug(msg)
      }
      cache += (key -> value)
      sender ! true

    case GetStringEntry(key) =>
      val value = cache.get(key)
      log.debug(s"GET: ($key, $value)")
      sender ! value

    case _ => sender ! Status.Failure(new IllegalArgumentException)
  }
}
