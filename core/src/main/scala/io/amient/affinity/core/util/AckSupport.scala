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

package io.amient.affinity.core.util

import java.util.concurrent.TimeoutException

import akka.AkkaException
import akka.actor.{ActorRef, Scheduler, Status}
import akka.pattern.{after, ask, pipe}
import akka.util.Timeout
import io.amient.affinity.core.http.RequestException
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.{implicitConversions, postfixOps}
import scala.runtime.BoxedUnit
import scala.util.control.NonFatal

/**
  * These are utilities for stateless Akka Ack pattern.
  * They are used where a chain of events has to be guaranteed to have completed.
  * For example, Coordinator identifies a new master and sends AddMaster to the respective Gateway.
  * Gateway in turn sends an ack message to the given Partition to BecomeMaster and
  * the partition must in turn ack that it has completed the transition successfully.
  * The response ack returns back up the chain until Coordinator is sure that the
  * partition has transitioned its state and knows it is now a Master.
  *
  * Supported delivery semantics: At-Least-Once
  *
  * Because this ack implementation is stateless, any logic relying on its functionality
  * must take care of deduplication that can result from the retries in case the ack response
  * wasn't delivered.
  *
  * Likewise in-order processing must be taken care of by the code relying on the ack.
  */
trait AckSupport {

  implicit def ack(actorRef: ActorRef): AckableActorRef = new AckableActorRef(actorRef)

}

trait Reply[+T] {

  def apply[S >: T](sender: ActorRef): ReplyTo[S] = new ReplyTo[S](sender)

}

class ReplyTo[T](sender: ActorRef) {
  def !(response: => T): Unit = {
    sender ! (try response catch {
      case NonFatal(e) => Status.Failure(e)
    })
  }

  def !(response: => Future[T])(implicit context: ExecutionContext): Future[T] = {
    (try response catch {
      case NonFatal(e) => Future.failed(e)
    }) pipeTo sender
  }
}

trait Scatter[T] extends Reply[T] {
  def gather(r1: T, r2: T): T
}

case class ScatterGather[T](msg: Scatter[T], timeout: Timeout) extends Reply[Iterable[T]]

final class AckableActorRef(val target: ActorRef) extends AnyRef {

  private val log = LoggerFactory.getLogger(this.getClass)


  /**
    * Typed Scatter Gather
    *
    * @param scatter
    * @param timeout
    * @param scheduler
    * @param context
    * @tparam T
    * @return gathered and aggregated result T
    */
  def ???[T](scatter: Scatter[T])(implicit timeout: Timeout, scheduler: Scheduler, context: ExecutionContext): Future[T] = {
    ??(ScatterGather(scatter, timeout)).map(_.reduce(scatter.gather))
  }

  /**
    * Typed Ask
    * @param message message ask to send this actor target
    * @param timeout after which the response is considered failed
    * @param context
    * @tparam T response type
    * @return
    */
  def ??[T](message: Reply[T])(implicit timeout: Timeout, context: ExecutionContext): Future[T] = {
    target ? message map (result => (if (result.isInstanceOf[BoxedUnit]) () else result).asInstanceOf[T])
  }

  /**
    * Ack - Typed Ask with Retries
    *
    * @param message
    * @param timeout
    * @param scheduler
    * @param context
    * @return Future response of type T
    */
  def ?![T](message: Reply[T])(implicit timeout: Timeout, scheduler: Scheduler, context: ExecutionContext): Future[T] = {
    val promise = Promise[T]()

    def attempt(retry: Int, delay: FiniteDuration = 0 seconds): Unit = {
      val f = if (delay.toMillis == 0) target ? message else after(delay, scheduler)(target ? message)
      f map {
        result => promise.success((if (result.isInstanceOf[BoxedUnit]) () else result).asInstanceOf[T])
      } recover {
        case cause: AkkaException => promise.failure(cause)
        case cause: RequestException => promise.failure(cause)
        case cause: NoSuchElementException => promise.failure(cause)
        case cause: IllegalArgumentException => promise.failure(cause)
        case cause: AssertionError => promise.failure(cause)
        case cause: IllegalAccessException => promise.failure(cause)
        case cause: SecurityException => promise.failure(cause)
        case cause: NotImplementedError => promise.failure(cause)
        case cause: UnsupportedOperationException => promise.failure(cause)
        case cause if (retry <= 0) =>
          log.error(s"${target.path.name} failed to respond to $message ")
          promise.failure(cause)
        case _: TimeoutException =>
          log.warn(s"Retrying $target ack $message due to timeout $timeout")
          attempt(retry - 1)
        case cause =>
          log.warn(s"Retrying $target ack $message due to: ", cause)
          attempt(retry - 1, timeout.duration)
      }
    }

    attempt(2)
    promise.future
  }

}