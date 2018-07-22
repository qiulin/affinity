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

import akka.AkkaException
import akka.actor.SupervisorStrategy.{Escalate, Restart, Resume}
import akka.actor.{Actor, InvalidActorNameException, OneForOneStrategy, Props, Terminated}
import akka.event.Logging
import io.amient.affinity.core.util.Reply

import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

object Controller {

  final case class CreateContainer(group: String, partitions: List[Int], partitionProps: Props) extends Reply[Unit]

  final case class ContainerOnline(group: String)

  final case class CreateGateway(handlerProps: Props) extends Reply[List[Int]]

  final case class GatewayCreated(httpPorts: List[Int])

  final case class FatalErrorShutdown(e: Throwable)

}

class Controller extends Actor {

  private val log = Logging.getLogger(context.system, this)

  import Controller._

  val system = context.system

  private var gatewayPromise: Promise[List[Int]] = null

  private val containers = scala.collection.mutable.Map[String, Promise[Unit]]()

  private implicit val executor = scala.concurrent.ExecutionContext.Implicits.global

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 1 minute) {
    case _: IllegalArgumentException ⇒ Resume
    case _: NoSuchElementException ⇒ Resume
    case _: NullPointerException ⇒ Resume
    case _: IllegalStateException ⇒ Resume
    case _: IllegalAccessException ⇒ Resume
    case _: SecurityException ⇒ Resume
    case _: NotImplementedError ⇒ Resume
    case _: UnsupportedOperationException ⇒ Resume
    case _: Exception ⇒ Restart
    case _: Throwable ⇒ Escalate
  }

  override def receive: Receive = {

    case request@CreateContainer(group, partitions, partitionProps) => request(sender) ! {
      try {
        log.debug(s"Creating Container for $group with partitions $partitions")
        context.actorOf(Props(new Container(group) {
          for (partition <- partitions) {
            context.actorOf(partitionProps, name = partition.toString)
          }
        }), name = group)
        val promise = Promise[Unit]()
        containers.put(group, promise)
        promise.future
      } catch {
        case _: InvalidActorNameException => containers(group).future
        case NonFatal(e) =>
          log.error(e, s"Could not create container for $group with partitions $partitions")
          throw e
      }
    }

    case Terminated(child) if (containers.contains(child.path.name)) =>
      val promise = containers(child.path.name)
      if (!promise.isCompleted) promise.failure(new AkkaException("Container initialisation failed"))

    case ContainerOnline(group) => containers(group) match {
      case promise => if (!promise.isCompleted) containers(group).success(())
    }

    case request@CreateGateway(gatewayProps) => try {
      val gatewayRef = context.actorOf(gatewayProps, name = "gateway")
      context.watch(gatewayRef)
      gatewayPromise = Promise[List[Int]]()
      request(sender) ! gatewayPromise.future
      gatewayRef ! CreateGateway
    } catch {
      case _: InvalidActorNameException => request(sender) ! gatewayPromise.future
    }

    case Terminated(child) if (child.path.name == "gateway") =>
      if (!gatewayPromise.isCompleted) gatewayPromise.failure(new AkkaException("Gateway initialisation failed"))

    case GatewayCreated(ports) => if (!gatewayPromise.isCompleted) {
      if (ports.length > 0) log.info("Gateway online (with http)") else log.info("Gateway online (stream only)")
      gatewayPromise.success(ports)
    }

    case anyOther => log.warning("Unknown controller message " + anyOther)
  }

}
