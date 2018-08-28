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

package io.amient.affinity.core.http

import java.io.IOException
import java.net.URI
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import akka.actor.{ActorRef, PoisonPill}
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.core.actor.{RegisterMediatorSubscriber, _}
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.http.RequestMatchers._
import io.amient.affinity.ws.WebSocketClient
import io.amient.affinity.ws.WebSocketClient.{AvroMessageHandler, JsonMessageHandler, TextMessageHandler}
import org.apache.avro.generic.GenericData
import org.codehaus.jackson.JsonNode
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.language.postfixOps


class WebSocketSupportSpec extends WordSpecLike with BeforeAndAfterAll with Matchers {


  val node = new Node(ConfigFactory.load("websocketsupportspec"))

  val specTimeout = 6 seconds

  import node.system.dispatcher

  implicit val materializer = ActorMaterializer.create(node.system)

  def httpPort = node.getHttpPort(0)

  def http_get(uri: Uri): String = {
    val promise = Promise[HttpResponse]()
    node.system.actorSelection("/user/controller/gateway") ! HttpExchange(HttpRequest(GET, uri), promise)
    val response = Await.result(promise.future flatMap (_.toStrict(2 second)), 2 seconds)
    Await.result(response.entity.dataBytes.runWith(Sink.head), 1 second).utf8String
  }


  override def beforeAll(): Unit = {
    node.start()
    node.awaitClusterReady()
  }

  override def afterAll: Unit = node.shutdown()

  "AvroWebSocket channel" must {

    "throw exception for unknown type schema request" in {
      (try {
        val ws = new WebSocketClient(URI.create(s"ws://127.0.0.1:$httpPort/test-avro-socket/100"), new AvroMessageHandler() {
          override def onMessage(message: scala.Any): Unit = ()

          override def onError(e: Throwable): Unit = ()
        })
        try {
          ws.getSchema("io.amient.affinity.core.http.WrongClass")
          false
        } finally {
          ws.close()
        }
      } catch {
        case _: IOException => true
      }) should be(true)
    }

    "retrieve valid schema for known type and forward a receive objects to and from the keyaspace according to that schema" in {
      val wsqueue = new LinkedBlockingQueue[AnyRef]()
      val ws = new WebSocketClient(URI.create(s"ws://127.0.0.1:$httpPort/test-avro-socket/101"), new AvroMessageHandler() {
        override def onError(e: Throwable): Unit = e.printStackTrace()

        override def onMessage(message: AnyRef): Unit = if (message != null) wsqueue.add(message)
      })
      try {
        val schema = ws.getSchema(classOf[Envelope].getName)
        schema should equal(AvroRecord.inferSchema(classOf[Envelope]))
        ws.send(Envelope(ID(101), Side.LEFT, Seq(ID(2000))))
        val push1 = wsqueue.poll(specTimeout.length, TimeUnit.SECONDS)
        push1 should not be (null)
        val record = push1.asInstanceOf[GenericData.Record]
        record.get("id").asInstanceOf[GenericData.Record].get("id") should be(101)
        record.get("side").toString should be("LEFT")
        record.get("seq").asInstanceOf[GenericData.Array[GenericData.Record]].size should be(1)
        record.get("seq").asInstanceOf[GenericData.Array[GenericData.Record]].get(0).get("id") should be(2000)
      } finally {
        ws.close()
      }
    }

//FIXME Verify that this feature doesn't make sense
//    "handle received messages with custom handler if defined at the partition level" in {
//      val wsqueue = new LinkedBlockingQueue[AnyRef]()
//      val ws = new WebSocketClient(URI.create(s"ws://127.0.0.1:$httpPort/test-avro-socket/102"), new AvroMessageHandler() {
//        override def onError(e: Throwable): Unit = e.printStackTrace()
//
//        override def onMessage(message: AnyRef): Unit = if (message != null) wsqueue.add(message)
//      })
//      try {
//        ws.getSchema(classOf[ID].getName)
//        ws.send(ID(102))
//        val push1 = wsqueue.poll(specTimeout.length, TimeUnit.SECONDS)
//        push1 should not be (null)
//        val record = push1.asInstanceOf[GenericData.Record]
//        record.getSchema.getFullName should be(classOf[ID].getName)
//        record.get("id") should be(103)
//      } finally {
//        ws.close()
//      }
//    }
  }

  "Json WebSocket channel" must {
    "receive json updates from the connected key-value" in {
      val wsqueue = new LinkedBlockingQueue[JsonNode]()
      val ws = new WebSocketClient(URI.create(s"ws://127.0.0.1:$httpPort/test-json-socket"), new JsonMessageHandler() {
        override def onError(e: Throwable): Unit = e.printStackTrace()

        override def onMessage(message: JsonNode) = wsqueue.add(message)
      })
      try {
        val push1 = wsqueue.poll(specTimeout.length, TimeUnit.SECONDS)
        push1 should be(Decoder.json("{}"))
        http_get(Uri(s"http://127.0.0.1:$httpPort/update-it"))
        val push2 = wsqueue.poll(specTimeout.length, TimeUnit.SECONDS)
        push2 should be(Decoder.json("{\"type\":\"io.amient.affinity.core.http.Envelope\",\"data\":{\"id\":{\"id\":2},\"side\":\"RIGHT\",\"seq\":[]}}"))
      } finally {
        ws.close()
      }
    }
  }

  "Custom WebSocket channel" must {
    "work" in {
      val wsqueue = new LinkedBlockingQueue[String]()
      val ws = new WebSocketClient(URI.create(s"ws://127.0.0.1:$httpPort/test-custom-socket"), new TextMessageHandler() {
        override def onError(e: Throwable): Unit = e.printStackTrace()

        override def onMessage(message: String) = wsqueue.add(message)
      })
      try {
        ws.send("Hello")
        wsqueue.poll(specTimeout.length, TimeUnit.SECONDS) should be("Welcome")
        wsqueue.poll(specTimeout.length, TimeUnit.SECONDS) should be("Here is your token")
        wsqueue.poll(specTimeout.length, TimeUnit.SECONDS) should be("{}") //initial value of the key
        ws.send("Write")
        wsqueue.poll(specTimeout.length, TimeUnit.SECONDS) should be("{\"type\":\"io.amient.affinity.core.http.ID\",\"data\":{\"id\":4}}")

      } finally {
        ws.close()
      }
    }
  }

}

class WebSocketSpecGateway extends GatewayHttp with WebSocketSupport {
  private val log: LoggingAdapter = Logging.getLogger(context.system, this)

  val regionService = keyspace("region")

  import context.dispatcher

  override def handle: Receive = {

    case WEBSOCK(PATH("test-avro-socket", INT(x)), _, socket) =>
      connectKeyValueMediator(regionService, "test", x) map {
        case keyValueMediator => avroWebSocket(socket, keyValueMediator)
      }

    case HTTP(HttpMethods.GET, PATH("update-it"), _, response) =>
      implicit val specTimeout = Timeout(6 seconds)
      accept(response, regionService ? Envelope(ID(2), Side.RIGHT))

    case WEBSOCK(PATH("test-json-socket"), _, socket) =>
      connectKeyValueMediator(regionService, "test", 2) map {
        case keyValueMediator => jsonWebSocket(socket, keyValueMediator)
      }

    case WEBSOCK(PATH("test-custom-socket"), _, socket) =>
      customWebSocket(socket, new DownstreamActor {

        private var mediator: ActorRef = null

        override def onClose(upstream: ActorRef): Unit = if (mediator != null) mediator ! PoisonPill

        override def receiveMessage(upstream: ActorRef): PartialFunction[Message, Unit] = {
          case TextMessage.Strict("Hello") =>
            connectKeyValueMediator(regionService, "test", 3) map {
              case keyValueMediator =>
                log.info(s"subscribing to $keyValueMediator")
                this.mediator = keyValueMediator
                upstream ! TextMessage.Strict("Welcome")
                upstream ! TextMessage.Strict("Here is your token")
                keyValueMediator ! RegisterMediatorSubscriber(upstream)
            }
          case msg if mediator == null => log.warning(s"IGNORING DOWNSTREAM - MEDIATOR NOT CONNECTED: $msg")
          case TextMessage.Strict("Write") if mediator != null =>
            implicit val timeout = Timeout(500 millis)
            (mediator ? ID(3)).map(upstream ! _)

        }

      }, new UpstreamActor {
        override def handle: Receive = {
          case None => push("{}")
          case Some(base: Envelope) => push(Encoder.json(base))
          case id: ID => push(Encoder.json(id))
        }
      })

  }
}


class WebSocketSpecPartition extends Partition {
  val data = state[Int, Envelope]("test")
  import context.dispatcher
  override def handle: Receive = {
    case query@Envelope(id, _, _) => query(sender) ! data.replace(id.id, query)
    case ID(s) => sender ! ID(s + 1)
  }
}