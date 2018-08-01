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

import java.io.File
import java.nio.file.Files
import java.security.cert.CertificateFactory
import java.security.{KeyStore, SecureRandom}
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.zip.GZIPInputStream

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.HttpEncodings
import akka.http.scaladsl.{ConnectionContext, Http}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.StreamConverters._
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.amient.affinity.Conf
import io.amient.affinity.avro.ZookeeperSchemaRegistry.ZkAvroConf
import io.amient.affinity.core.cluster.CoordinatorZk.CoordinatorZkConf
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.http.Encoder
import javax.net.ssl.{SSLContext, TrustManagerFactory}
import org.apache.avro.util.ByteBufferInputStream
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.{implicitConversions, postfixOps}

object AffinityTestBase {
  val akkaPort = new AtomicInteger(15001)
  var embeddedCoordinatorId = new AtomicLong(1)
}

trait AffinityTestBase {

  implicit def nodeToNodeWithTestMethods(node: Node): NodeWithTestMethods = new NodeWithTestMethods(node)

  final def configure(): Config = configure(ConfigFactory.empty)

  final def configure(config: Config): Config = configure(config, None, None)

  final def configure(confname: String, zkConnect: Option[String] = None, kafkaBootstrap: Option[String] = None): Config = {
    configure(ConfigFactory.load(confname), zkConnect, kafkaBootstrap)
  }

  def configure(config: Config, zkConnect: Option[String], kafkaBootstrap: Option[String]): Config = {
    val layer1: Config = if (config.hasPath(Conf.Affi.SystemName.path)) config else {
      config.withValue(Conf.Affi.SystemName.path, ConfigValueFactory.fromAnyRef(UUID.randomUUID().toString))
    }

    val layer2: Config = layer1
      .withValue(Conf.Akka.Port.path, ConfigValueFactory.fromAnyRef(AffinityTestBase.akkaPort.getAndIncrement()))

    val layer3: Config = zkConnect match {
      case None => layer2
      case Some(zkConnectString) =>
        layer2
          .withValue(CoordinatorZkConf(Conf.Affi.Coordinator).ZooKeeper.Connect.path, ConfigValueFactory.fromAnyRef(zkConnectString))
          .withValue(ZkAvroConf(Conf.Affi.Avro).ZooKeeper.Connect.path, ConfigValueFactory.fromAnyRef(zkConnectString))
    }

    kafkaBootstrap match {
      case None => layer3
      case Some(kafkaBootstrapString) =>
        val keySpaceStores = if (!layer3.hasPath(Conf.Affi.Keyspace.path())) List.empty else layer3
          .getObject(Conf.Affi.Keyspace.path()).keySet().asScala
          .flatMap { ks =>
            layer3.getObject(Conf.Affi.Keyspace(ks).State.path).keySet().asScala.map {
              case stateName => Conf.Affi.Keyspace(ks).State(stateName).path()
            }
          }

        val globalStores = if (!layer3.hasPath(Conf.Affi.Global.path())) List.empty else layer3
          .getObject(Conf.Affi.Global.path()).keySet().asScala
          .map { ks => Conf.Affi.Global(ks).path }

        (keySpaceStores ++ globalStores).foldLeft(layer3) {
          case (c, stateStorePath) =>
            val stateConfig = c.getConfig(stateStorePath)
            if (!stateConfig.getString("storage.class").toLowerCase.contains("kafka")) c else {
              c.withValue(s"$stateStorePath.storage.kafka.bootstrap.servers",
                ConfigValueFactory.fromAnyRef(kafkaBootstrapString))
            }
        }
    }
  }

  def createTempDirectory: File = Files.createTempDirectory(this.getClass.getSimpleName).toFile

  def deleteDirectory(f: File): Unit = if (f.exists) {
    if (f.isDirectory) f.listFiles.foreach(deleteDirectory)
    if (!f.delete) throw new RuntimeException(s"Failed to delete ${f.getAbsolutePath}")
  }

  def jsonStringEntity(s: String) = HttpEntity.Strict(ContentTypes.`application/json`, ByteString("\"" + s + "\""))

}


class NodeWithTestMethods(underlying: Node) {


  private implicit val system = underlying.system

  private implicit val materializer = ActorMaterializer.create(system)

  import system.dispatcher

  lazy val gateway = Await.result(system.actorSelection("/user/controller/gateway").resolveOne(FiniteDuration.apply(1, TimeUnit.SECONDS)), 1 second)

  val testSSLContext = {
    val certStore = KeyStore.getInstance(KeyStore.getDefaultType)
    certStore.load(null, null)
    certStore.setCertificateEntry("ca", CertificateFactory.getInstance("X.509")
      .generateCertificate(getClass.getClassLoader.getResourceAsStream("keys/localhost.cer")))
    val certManagerFactory = TrustManagerFactory.getInstance("SunX509")
    certManagerFactory.init(certStore)
    val context = SSLContext.getInstance("TLS")
    context.init(null, certManagerFactory.getTrustManagers, new SecureRandom)
    ConnectionContext.https(context)
  }

  def wsuri(path: String, interface: Int = 0) = new java.net.URI(s"ws://localhost:${underlying.getHttpPort(interface)}$path")

  def wssuri(path: String, interface: Int = 0) = new java.net.URI(s"wss://localhost:${underlying.getHttpPort(interface)}$path")

  def http(method: HttpMethod, path: String, interface: Int = 0): Future[HttpResponse] = {
    http(HttpRequest(method = method, uri(path, interface)))
  }

  def http_get(path: String, headers: List[HttpHeader], interface: Int): HttpResponse = {
    Await.result(http(HttpRequest(method = HttpMethods.GET, uri(path, interface), headers = headers)), 30 seconds)
  }

  def http_get(path: String, headers: List[HttpHeader]): HttpResponse = http_get(path, headers, interface = 0)

  def http_get(path: String, interface: Int): HttpResponse = http_get(path, List(), interface)

  def http_get(path: String): HttpResponse = http_get(path, interface = 0)

  def https_get(path: String, headers: List[HttpHeader], interface: Int): HttpResponse = {
    Await.result(http(HttpRequest(method = HttpMethods.GET, https_uri(path, interface), headers = headers)), 30 seconds)
  }

  def https_get(path: String, headers: List[HttpHeader]): HttpResponse = https_get(path, headers, interface = 0)

  def https_get(path: String, interface: Int): HttpResponse = https_get(path, List(), interface)

  def https_get(path: String): HttpResponse = https_get(path, interface = 0)

  val mapper = new ObjectMapper()

  def get_json(response: HttpResponse): JsonNode = {
    val json = Await.result(response.entity.dataBytes.runWith(Sink.head), 1 second).utf8String
    mapper.readValue(json, classOf[JsonNode])
  }

  def get_text(response: HttpResponse): String = {
    Await.result(response.entity.dataBytes.runWith(Sink.head), 1 second).utf8String
  }

  def http_post(path: String, entity: Array[Byte] = Array(), headers: List[HttpHeader] = List()): HttpResponse = {
    Await.result(http(HttpRequest(entity = HttpEntity(entity), method = HttpMethods.POST, uri = uri(path), headers = headers)), 30 seconds)
  }

  def http_post_json(path: String, json: JsonNode, headers: List[HttpHeader] = List()): HttpResponse = {
    Await.result(http(HttpRequest(entity = HttpEntity(ContentTypes.`application/json`, Encoder.json(json)), method = HttpMethods.POST, uri = uri(path), headers = headers)), 30 seconds)
  }

  def https_post(path: String, entity: Array[Byte] = Array(), headers: List[HttpHeader] = List()): HttpResponse = {
    Await.result(http(HttpRequest(entity = HttpEntity(entity), method = HttpMethods.POST, uri = https_uri(path), headers = headers)), 30 seconds)
  }

  def https_post_json(path: String, json: JsonNode, headers: List[HttpHeader] = List()): HttpResponse = {
    Await.result(http(HttpRequest(entity = HttpEntity(ContentTypes.`application/json`, Encoder.json(json)), method = HttpMethods.POST, uri = https_uri(path), headers = headers)), 30 seconds)
  }

  def uri(path: String, interface: Int = 0) = Uri(s"http://localhost:${underlying.getHttpPort(interface)}$path")

  def https_uri(path: String, interface: Int = 0) = Uri(s"https://localhost:${underlying.getHttpPort(interface)}$path")

  def http(req: HttpRequest) = {
    val decodedResponse: Future[HttpResponse] = Http().singleRequest(req, testSSLContext) flatMap {
      response =>
        response.header[headers.`Content-Encoding`] match {
          case Some(c) if (c.encodings.contains(HttpEncodings.gzip)) =>
            response.entity.dataBytes.map(_.asByteBuffer).runWith(Sink.seq).map {
              byteBufferSequence =>
                val unzipped = fromInputStream(() => new GZIPInputStream(new ByteBufferInputStream(byteBufferSequence.asJava)))
                val unzippedEntity = HttpEntity(response.entity.contentType, unzipped)
                response.copy(entity = unzippedEntity)
            }
          case _ => Future.successful(response)
        }
    }
    decodedResponse.flatMap(_.toStrict(2 seconds))
  }

}