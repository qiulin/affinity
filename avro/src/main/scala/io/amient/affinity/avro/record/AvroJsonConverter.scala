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

package io.amient.affinity.avro.record

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import io.amient.affinity.avro.record.AvroRecord.extract
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecordBuilder}
import org.apache.avro.util.Utf8
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper
import scala.reflect.runtime.universe.TypeTag
import scala.collection.JavaConverters._
import scala.util.Try

object AvroJsonConverter {

  def toJson(data: Any, pretty: Boolean = false): String = {
    val out = new ByteArrayOutputStream()
    toJson(out, data, pretty)
    out.toString()
  }

  def toJson(out: OutputStream, data: Any, pretty: Boolean): Unit = toJson(out, AvroRecord.inferSchema(data), data, pretty)

  def toJson(out: OutputStream, schema: Schema, data: Any, pretty: Boolean): Unit = {
    val encoder: JsonEncoder = new JsonEncoder(schema, out, pretty)
    val writer = new GenericDatumWriter[Any](schema)
    writer.write(extract(data, List(schema)), encoder)
    encoder.flush()
  }

  private val mapper = new ObjectMapper()

  def toAvro[T: TypeTag](json: String): T = {
    return toAvro(json, AvroRecord.inferSchema[T]).asInstanceOf[T]
  }

  def toAvro(json: String, schema: Schema): Any = {

    def to(json: JsonNode, schema: Schema): Any = try {
      schema.getType match {
        case Schema.Type.NULL if json == null || json.isNull => null
        case Schema.Type.BOOLEAN if json.isBoolean => json.getBooleanValue
        case Schema.Type.INT if json.isNumber => json.getIntValue
        case Schema.Type.LONG if json.isNumber => json.getLongValue
        case Schema.Type.FLOAT if json.isNumber => json.getDoubleValue.toFloat
        case Schema.Type.DOUBLE if json.isNumber => json.getDoubleValue
        case Schema.Type.STRING if json == null => new Utf8()
        case Schema.Type.STRING if json.isTextual => new Utf8(json.getTextValue)
        case Schema.Type.UNION if schema.getTypes.size == 2 && (schema.getTypes.get(0).getType == Schema.Type.NULL || schema.getTypes.get(1).getType == Schema.Type.NULL) =>
          schema.getTypes.asScala.map { s => Try(to(json, s)) }.find(_.isSuccess).map(_.get).get
        case Schema.Type.UNION =>
          val utype = json.getFieldNames.next
          schema.getTypes.asScala.filter(_.getFullName == utype).map(s => Try(to(json.get(utype), s))).find(_.isSuccess).map(_.get).get
        case Schema.Type.ARRAY if json.isArray => json.getElements.asScala.map(x => to(x, schema.getElementType)).toList.asJava
        case Schema.Type.MAP if json.isObject =>
          val builder = Map.newBuilder[Utf8, Any]
          json.getFields.asScala foreach { entry =>
            builder += new Utf8(entry.getKey) -> to(entry.getValue, schema.getValueType)
          }
          builder.result.asJava
        case Schema.Type.ENUM if json.isTextual => new EnumSymbol(schema, json.getTextValue)
        case Schema.Type.BYTES => ByteBuffer.wrap(json.getTextValue.getBytes(StandardCharsets.UTF_8))
        case Schema.Type.FIXED => new GenericData.Fixed(schema, json.getTextValue.getBytes(StandardCharsets.UTF_8))
        case Schema.Type.RECORD if json.isObject =>
          val builder = new GenericRecordBuilder(schema)
          schema.getFields.asScala foreach { field =>
            try {
              val d = json.get(field.name)
              builder.set(field, to(d, field.schema()))
            } catch {
              case e: Throwable => throw new RuntimeException(s"Can't convert json field `$field` with value ${json.get(field.name)} using schema: ${field.schema}", e)
            }
          }
          builder.build()
        case x => throw new IllegalArgumentException(s"Unsupported schema type `$x`")
      }
    } catch {
      case e: Throwable => throw new RuntimeException(s"Can't convert ${json} using schema: $schema", e)
    }

    AvroRecord.read(to(mapper.readTree(json), schema), schema)
  }
}
