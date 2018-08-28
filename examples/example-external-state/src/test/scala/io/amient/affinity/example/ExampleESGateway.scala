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

package io.amient.affinity.example

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorRef
import akka.http.scaladsl.model.HttpMethods.GET
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.{GatewayHttp, Partition}
import io.amient.affinity.core.http.RequestMatchers.{HTTP, PATH}
import io.amient.affinity.core.util.Scatter

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class ExampleESGateway extends GatewayHttp {

  val keyspace: ActorRef = keyspace("external")

  import context.dispatcher
  implicit val scheduler = context.system.scheduler
  override def handle: Receive = {

    case HTTP(GET, PATH("news", "latest"), _, response) =>
      implicit val timeout = Timeout(5 seconds)
      val latestNews: Future[List[String]] = keyspace ?? GetLatest()
      val responseText = latestNews.map(news=> "LATEST NEWS:\n" + news.mkString("\n"))
      handleAsText(response, responseText)
  }

}


case class GetLatest() extends Scatter[List[String]] {
  override def gather(r1: List[String], r2: List[String]) = r1 ++ r2
}


class ExampleESPartition extends Partition {


  val latest = new ConcurrentLinkedQueue[(String, String)]()

  state[String, String]("news").listen {
    case record if record.value != null =>
      latest.add((record.key, record.value))
      while (latest.size() > 3) latest.poll()
  }

  override def handle: Receive = {
    case request@GetLatest() =>
      request(sender) ! latest.iterator.asScala.map(_.productIterator.mkString(("\t"))).toList

  }
}
