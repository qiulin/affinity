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

package io.amient.affinity.core.serde.avro

import akka.actor.ExtendedActorSystem
import com.typesafe.config.Config
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.core.serde.{Serde, Serdes}

final class AvroSerdeProxy(config: Config) extends Serde[Any] {

  def this(system: ExtendedActorSystem) = this(system.settings.config)
  def this(serdes: Serdes) = this(serdes.config)

  val internal = AvroSerde.create(config)

  override def fromBytes(bytes: Array[Byte]): Any = internal.fromBytes(bytes)

  override def toBytes(obj: Any): Array[Byte] = internal.toBytes(obj)

  override def close(): Unit = if (internal != null) internal.close()

  override def identifier: Int = 200

  override def prefix(cls: Class[_ <: Any], prefix: Object*): Array[Byte] = internal.prefix(cls, prefix:_*)

}

