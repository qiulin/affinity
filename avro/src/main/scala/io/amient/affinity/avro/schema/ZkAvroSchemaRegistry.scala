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

package io.amient.affinity.avro.schema

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.avro.AvroSerde
import io.amient.affinity.avro.AvroSerde.AvroConf
import io.amient.affinity.avro.schema.ZkAvroSchemaRegistry.ZkAvroConf
import io.amient.affinity.core.config.{Cfg, CfgStruct}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.apache.avro.{Schema, SchemaValidatorBuilder}
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConversions._
import scala.collection.immutable

object ZkAvroSchemaRegistry {

  object Conf extends Conf

  class Conf extends CfgStruct[Conf](Cfg.Options.IGNORE_UNKNOWN) {
    val Avro = struct("affinity.avro", new ZkAvroConf, false)
  }

  class ZkAvroConf extends CfgStruct[ZkAvroConf](classOf[AvroConf]) {
    val Connect = string("schema.registry.zookeeper.connect", true)
    val Root = string("schema.registry.zookeeper.root", true)
    val ConnectTimeoutMs = integer("schema.registry.zookeeper.timeout.connect.ms", true)
    val SessionTimeoutMs = integer("schema.registry.zookeeper.timeout.session.ms", true)
  }

}

class ZkAvroSchemaRegistry(config: Config) extends AvroSerde with AvroSchemaProvider {
  val merged = config.withFallback(ConfigFactory.defaultReference().getConfig(AvroSerde.Conf.Avro.path))
  val conf = new ZkAvroConf().apply(merged)
  private val zkRoot = conf.Root()

  private val zk = new ZkClient(conf.Connect(), conf.SessionTimeoutMs(), conf.ConnectTimeoutMs(), new ZkSerializer {
      def serialize(o: Object): Array[Byte] = o.toString.getBytes

      override def deserialize(bytes: Array[Byte]): Object = new String(bytes)
    })


  private val validator = new SchemaValidatorBuilder().mutualReadStrategy().validateLatest()

  @volatile private var internal = immutable.Map[String, List[(Int, Schema)]]()

  if (!zk.exists(zkRoot)) zk.createPersistent(zkRoot, true)
  updateInternal(zk.subscribeChildChanges(zkRoot, new IZkChildListener() {
    override def handleChildChange(parentPath: String, children: util.List[String]): Unit = {
      updateInternal(children)
    }
  }))

  override def close(): Unit = zk.close()

  override private[schema] def registerSchema(subject: String, schema: Schema, existing: List[Schema]): Int = {
    validator.validate(schema, existing)
    val path = zk.create(s"$zkRoot/", schema.toString(true), CreateMode.PERSISTENT_SEQUENTIAL)
    val id = path.substring(zkRoot.length + 1).toInt
    id
  }

  override private[schema] def getAllRegistered: List[(Int, String, Schema)] = {
    val ids = zk.getChildren(zkRoot)
    ids.toList.map { id =>
      val schema = new Schema.Parser().parse(zk.readData[String](s"$zkRoot/$id"))
      val schemaId = id.toInt
      (schemaId, schema.getFullName, schema) //TODO schema.getFullName is a lazy subject, we need /zkRoot/subjects + /zkRoot/schemas
    }
  }

  override private[schema] def hypersynchronized[X](f: => X): X = synchronized {
    val lockPath = zkRoot + "-lock"
    var acquired = 0
    do {
      try {
        zk.createEphemeral(lockPath)
        acquired = 1
      } catch {
        case _: ZkNodeExistsException =>
          acquired -= 1
          if (acquired < -100) {
            throw new IllegalStateException("Could not acquire zk registry lock")
          } else {
            Thread.sleep(500)
          }
      }
    } while (acquired != 1)
    try f finally zk.delete(lockPath)
  }

  private def updateInternal(ids: util.List[String]): Unit = {
    internal = ids.toList.map { id =>
      val schema = new Schema.Parser().parse(zk.readData[String](s"$zkRoot/$id"))
      val FQN = schema.getFullName
      val schemaId = id.toInt
      (FQN, (schemaId, schema))
    }.groupBy(_._1).mapValues(_.map(_._2))
  }

}