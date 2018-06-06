package io.amient.affinity.avro

import com.example.domain.ClickEvent
import org.scalatest.{FlatSpec, Matchers}

class HugeSchemaSpec extends FlatSpec with Matchers {

  val serde= new MemorySchemaRegistry()

  it should "work" in {

    val e = ClickEvent()

    val bytes = serde.toBytes(e)

    serde.fromBytes(bytes) should be(e)
  }

}
