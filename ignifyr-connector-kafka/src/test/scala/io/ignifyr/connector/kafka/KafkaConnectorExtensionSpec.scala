package io.ignifyr.connector.kafka

import io.ignifyr.engine.model.{KafkaSource, KafkaSourceSettings}
import io.ignifyr.engine.spi.ExtensionRegistry
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Verifies the Kafka connector is discovered through ServiceLoader when this module is on the
 * classpath — both its source connector and its streaming-failure descriptor (no Docker required).
 */
class KafkaConnectorExtensionSpec extends AnyFlatSpec with Matchers {

  "The Kafka connector extension" should "register a KafkaSource connector through ServiceLoader" in {
    val connector = ExtensionRegistry.sourceConnectors.get(classOf[KafkaSource])
    connector.map(_.id) shouldBe Some("kafka")
    connector.map(_.settingsClass) shouldBe Some(classOf[KafkaSourceSettings])
  }

  it should "register a streaming-failure descriptor" in {
    ExtensionRegistry.streamingFailureDescriptors should not be empty
  }
}
