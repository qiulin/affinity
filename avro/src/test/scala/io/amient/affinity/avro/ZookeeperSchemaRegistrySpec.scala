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

package io.amient.affinity.avro

import io.amient.affinity.avro.MemorySchemaRegistry.MemAvroConf
import io.amient.affinity.avro.ZookeeperSchemaRegistry.ZkAvroConf
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.kafka.EmbeddedZooKeeper
import org.apache.avro.{Schema, SchemaValidationException}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class ZookeeperSchemaRegistrySpec extends FlatSpec with Matchers with EmbeddedZooKeeper {

  behavior of "ZkAvroRegistry"

  val v1schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record_Current\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"SimpleRecord\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"record\",\"name\":\"SimpleKey\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]},\"default\":{\"id\":0}},{\"name\":\"side\",\"type\":{\"type\":\"enum\",\"name\":\"SimpleEnum\",\"symbols\":[\"A\",\"B\",\"C\"]},\"default\":\"A\"},{\"name\":\"seq\",\"type\":{\"type\":\"array\",\"items\":\"SimpleKey\"},\"default\":[]}]}},\"default\":[]},{\"name\":\"removed\",\"type\":\"int\",\"default\":0}]}")
  val v3schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record_Current\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"SimpleRecord\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"record\",\"name\":\"SimpleKey\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]},\"default\":{\"id\":0}},{\"name\":\"side\",\"type\":{\"type\":\"enum\",\"name\":\"SimpleEnum\",\"symbols\":[\"A\",\"B\",\"C\"]},\"default\":\"A\"},{\"name\":\"seq\",\"type\":{\"type\":\"array\",\"items\":\"SimpleKey\"},\"default\":[]}]}},\"default\":[]},{\"name\":\"index\",\"type\":{\"type\":\"map\",\"values\":\"SimpleRecord\"},\"default\":{}}]}")

  val conf = AvroConf(Map(
    AvroConf.Class.path -> classOf[ZookeeperSchemaRegistry].getName,
    ZkAvroConf(AvroConf).ZooKeeper.Connect.path -> zkConnect
  ).asJava)

  val serde = AvroSerde.create(conf)
  serde.register[SimpleKey]
  serde.register[SimpleRecord]
  val backwardSchemaId = serde.register[Record_Current](v1schema)
  val currentSchemaId = serde.register[Record_Current]
  val forwardSchemaId = serde.register[Record_Current](v3schema)

  it should "work in a backward-compatibility scenario" in {
    val oldValue = Record_V1(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.C)), 10)
    val oldBytes = serde.write(oldValue, v1schema, backwardSchemaId)
    oldBytes.mkString(",") should be("0,0,0,0,10,2,2,4,0,0,20")
    val upgraded = serde.fromBytes(oldBytes)
    upgraded should be(Record_Current(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.C)), Map()))
  }

  it should "work in a forward-compatibility scenario" in {
    val forwardValue = Record_V3(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.A)), Map("X" -> SimpleRecord(SimpleKey(1), SimpleEnum.A)))
    val forwardBytes = serde.write(forwardValue, v3schema, forwardSchemaId)
    val downgraded = serde.fromBytes(forwardBytes)
    downgraded should be(Record_Current(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.A)), Map("X" -> SimpleRecord(SimpleKey(1), SimpleEnum.A)), Set()))
  }

  it should "reject incompatible schema registration" in {
    val v4schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"data\",\"type\":\"string\"}]}")
    an[SchemaValidationException] should be thrownBy {
      serde.register[Record_Current](v4schema)
    }
  }

}
