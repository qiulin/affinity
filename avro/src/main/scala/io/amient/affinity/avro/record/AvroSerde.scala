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

package io.amient.affinity.avro.record

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer
import java.util

import com.typesafe.config.Config
import io.amient.affinity.avro.AvroSchemaRegistry
import io.amient.affinity.avro.record.AvroSerde.MAGIC
import io.amient.affinity.core.config.{Cfg, CfgStruct}
import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.core.util.ByteUtils
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord, IndexedRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.util.ByteBufferInputStream

import scala.collection.JavaConversions._

object AvroSerde {

  private val MAGIC: Byte = 0

  object Conf extends Conf {
    override def apply(config: Config): Conf = new Conf().apply(config)
  }

  class Conf extends CfgStruct[Conf](Cfg.Options.IGNORE_UNKNOWN) {
    val Avro = struct("affinity.avro", new AvroConf, false)
  }
  class AvroConf extends CfgStruct[AvroConf] {
    val Class = cls("schema.registry.class", classOf[AvroSerde], true)
    override protected def specializations(): util.Set[String] = {
      Set("schema.registry")
    }
  }

  def create(config: Config): AvroSerde = create(Conf(config).Avro)

  def create(conf: AvroConf): AvroSerde = {
    val registryClass: Class[_ <: AvroSerde] = conf.Class()
    val instance = try {
      registryClass.getConstructor(classOf[Config]).newInstance(conf.config())
    } catch {
      case _: NoSuchMethodException => registryClass.newInstance()
    }
    instance.initialize()
    instance
  }
}

trait AvroSerde extends AbstractSerde[Any] with AvroSchemaRegistry {

  override def close(): Unit = ()

  /**
    * Deserialize bytes to a concrete instance
    * @param bytes
    * @return AvroRecord for registered Type
    *         GenericRecord if no type is registered for the schema retrieved from the schemaRegistry
    *         null if bytes are null
    */
  override def fromBytes(bytes: Array[Byte]): Any = read(bytes)

  /**
    * @param obj instance to serialize
    * @return serialized byte array
    */
  override def toBytes(obj: Any): Array[Byte] = {
    if (obj == null) null
    else {
      val (s, schemaId) = getOrRegisterSchema(obj)
      write(obj, s, schemaId)
    }
  }

  def getOrRegisterSchema(data: Any, subjectOption: String = null): (Schema, Int) = {
    val schema = AvroRecord.inferSchema(data)
    val subject = Option(subjectOption).getOrElse(schema.getFullName)
    val schemaId = getCurrentSchema(schema.getFullName) match {
      case Some((schemaId: Int, regSchema: Schema)) if regSchema == schema => schemaId
      case _ =>
        register(schema.getFullName, schema)
        initialize()
        getSchemaId(schema) match {
          case None => throw new IllegalStateException(s"Failed to register schema for $subject")
          case Some(id) => id
        }
    }
    if (!getSchema(subject, schemaId).isDefined) {
      register(subject, schema)
      initialize()
    }
    (schema, schemaId)
  }

  def write(x: IndexedRecord, schemaId: Int): Array[Byte] = {
    write(x, x.getSchema, schemaId)
  }

  def write(value: Any, schema: Schema, schemaId: Int): Array[Byte] = {
    require(schemaId >= 0)
    value match {
      case null => null
      case record:AvroRecord if record._serializedInstanceBytes != null => record._serializedInstanceBytes
      case any: Any =>
        val valueOut = new ByteArrayOutputStream()
        try {
          valueOut.write(MAGIC)
          ByteUtils.writeIntValue(schemaId, valueOut)
          AvroRecord.write(value, schema, valueOut)
          valueOut.toByteArray
        } finally {
          valueOut.close
        }
    }
  }

  /**
    *
    * @param buf ByteBuffer version of the registered avro reader
    *
    * @return AvroRecord for registered Type
    *         GenericRecord if no type is registered for the schema retrieved from the schemaRegistry
    *         null if bytes are null
    */
  def read(buf: ByteBuffer): Any = {
    if (buf == null) null else read(new ByteBufferInputStream(List(buf)))
  }

  /**
    *
    * @param bytes
    * @return AvroRecord for registered Type
    *         GenericRecord if no type is registered for the schema retrieved from the schemaRegistry
    *         null if bytes are null
    */
  def read(bytes: Array[Byte]): Any = {
    if (bytes == null) null else read(new ByteArrayInputStream(bytes))  match {
      case a: AvroRecord => a._serializedInstanceBytes = bytes; a
      case other => other
    }
  }


  /**
    *
    * @param bytesIn InputStream implementation for the registered avro reader
    * @return AvroRecord for registered Type
    *         GenericRecord if no type is registered for the schema retrieved from the schemaRegistry
    *         null if bytes are null
    */
  def read(bytesIn: InputStream): Any = {
    require(bytesIn.read() == AvroSerde.MAGIC)
    val schemaId = ByteUtils.readIntValue(bytesIn)
    require(schemaId >= 0)
    val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(bytesIn, null)
    schema(schemaId) match {
      case None => throw new IllegalArgumentException(s"Schema $schemaId doesn't exist")
      case Some(writerSchema) =>
        getCurrentSchema(writerSchema.getFullName) match {
          case None =>
            //case classes are not present in this runtime, just use GenericRecord
            val reader = new GenericDatumReader[GenericRecord](writerSchema, writerSchema)
            reader.read(null, decoder)
          case Some((_, readerSchema)) =>
            //http://avro.apache.org/docs/1.7.2/api/java/org/apache/avro/io/parsing/doc-files/parsing.html
            val reader = new GenericDatumReader[Any](writerSchema, readerSchema)
            val record = reader.read(null, decoder)
            AvroRecord.read(record, readerSchema)
        }
    }
  }


}