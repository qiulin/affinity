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

import akka.http.scaladsl.model.HttpMethods
import akka.util.Timeout
import io.amient.affinity.avro.record.{AvroRecord, Fixed}
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.{GatewayHttp, GatewayStream, Partition, Routed}
import io.amient.affinity.core.http.RequestMatchers.{HTTP, INT, PATH, QUERY}
import io.amient.affinity.core.storage.Record
import io.amient.affinity.core.util.{EventTime, Reply, Scatter, TimeRange}

import scala.concurrent.duration._
import scala.language.postfixOps

case class Account(sortcode: String, number: Int) extends AvroRecord
case class Transaction(id: Long, amount: Double, timestamp: Long) extends AvroRecord with EventTime {
  override def eventTimeUnix() = timestamp
}


class ExampleBank extends GatewayStream with GatewayHttp {

  implicit val executor = context.dispatcher
  implicit val scheduler = context.system.scheduler
  implicit val timeout = Timeout(5 seconds)

  val defaultKeyspace = keyspace("default")

  input[Account, Transaction]("input-stream") { record: Record[Account, Transaction] =>
    defaultKeyspace ack StoreTransaction(record.key, record.value)
  }

  override def handle: Receive = {
    case HTTP(HttpMethods.GET, PATH("transactions", sortcode, INT(number)), _, response) =>
      defaultKeyspace ack GetAccountTransactions(Account(sortcode, number)) map (handleAsJson(response, _))

    case HTTP(HttpMethods.GET, PATH("transactions", sortcode), QUERY(("before", before)), response) =>
      defaultKeyspace gather GetBranchTransactions(sortcode, EventTime.unix(before+"T00:00:00+00:00")) map (handleAsJson(response, _))

    case HTTP(HttpMethods.GET, PATH("transactions", sortcode), _, response) =>
      defaultKeyspace gather GetBranchTransactions(sortcode) map (handleAsJson(response, _))
  }

}


case class StoreTransaction(key: Account, t: Transaction) extends AvroRecord with Routed with Reply[Unit]
case class StorageKey(@Fixed(8) sortcode: String, @Fixed account: Int, txn: Long) extends AvroRecord
case class GetAccountTransactions(key: Account) extends AvroRecord with Routed with Reply[Seq[Transaction]]
case class GetBranchTransactions(sortcode: String, beforeUnixTs: Long = Long.MaxValue) extends AvroRecord with Scatter[Seq[Transaction]] {
  override def gather(r1: Seq[Transaction], r2: Seq[Transaction]) = r1 ++ r2
}

class DefaultPartition extends Partition {

  val transactions = state[StorageKey, Transaction]("transactions")

  override def handle: Receive = {

    case request@StoreTransaction(account@Account(sortcode, number), transaction) => sender.reply(request) {
      transactions.replace(StorageKey(sortcode, number, transaction.id), transaction)
    }

    case request@GetBranchTransactions(sortcode, before) => sender.reply(request) {
      transactions.range(TimeRange.until(before), sortcode).values.toList
    }

    case request@GetAccountTransactions(account) => sender.reply(request) {
      transactions.range(TimeRange.UNBOUNDED, account.sortcode, account.number).values.toList
    }
  }
}