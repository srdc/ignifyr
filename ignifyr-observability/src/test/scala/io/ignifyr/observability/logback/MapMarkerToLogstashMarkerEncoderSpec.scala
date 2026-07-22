package io.ignifyr.observability.logback

import net.logstash.logback.encoder.LogstashEncoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Smoke test: the encoder loads and is a [[LogstashEncoder]], so an enterprise logback.xml can
 * reference it as an encoder. Its marker-conversion behaviour is exercised end-to-end when the
 * server boots its Fluentd/Logstash logback pipeline.
 */
class MapMarkerToLogstashMarkerEncoderSpec extends AnyFlatSpec with Matchers {

  behavior of "MapMarkerToLogstashMarkerEncoder"

  it should "be a LogstashEncoder that instantiates without configuration" in {
    val encoder = new MapMarkerToLogstashMarkerEncoder
    encoder shouldBe a[LogstashEncoder]
  }
}
