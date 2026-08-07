package io.ignifyr.observability.logback

import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.classic.{Level, LoggerContext}
import ch.qos.logback.more.appenders.marker.MapMarker
import net.logstash.logback.encoder.LogstashEncoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util

/**
 * This encoder is the whole of the observability module's logic and the only thing that gets the engine's
 * structured execution markers into Elasticsearch: the engine logs a `MapMarker` (a plain map), which
 * Logstash's encoder does not understand, so this class rewrites it into a `LogstashMarker` before
 * delegating. If the rewrite silently stopped happening, the Kibana "Executions" dashboard would go blank
 * while every log line still looked fine.
 *
 * The class is referenced by name from the server's logback.xml, so it is not reachable from Scala.
 */
class MapMarkerToLogstashMarkerEncoderSpec extends AnyFlatSpec with Matchers {

  private val loggerContext = new LoggerContext()

  private def startedEncoder: MapMarkerToLogstashMarkerEncoder = {
    val encoder = new MapMarkerToLogstashMarkerEncoder
    encoder.setContext(loggerContext)
    encoder.start()
    encoder
  }

  /** A warn-level event carrying the given marker, shaped like the ones `SinkHandler` emits. */
  private def event(marker: org.slf4j.Marker): LoggingEvent = {
    val loggingEvent = new LoggingEvent()
    loggingEvent.setLoggerName("io.ignifyr.engine.data.write.SinkHandler")
    loggingEvent.setLevel(Level.WARN)
    loggingEvent.setMessage("Mapping failure")
    loggingEvent.setTimeStamp(System.currentTimeMillis())
    loggingEvent.setLoggerContextRemoteView(loggerContext.getLoggerContextRemoteView)
    loggingEvent.setMarker(marker)
    loggingEvent
  }

  private def mapMarker(entries: (String, Any)*): MapMarker = {
    val map: util.Map[String, Any] = new util.HashMap[String, Any]()
    entries.foreach { case (key, value) => map.put(key, value) }
    new MapMarker("marker", map)
  }

  private def encode(marker: org.slf4j.Marker): String =
    new String(startedEncoder.encode(event(marker)), StandardCharsets.UTF_8)

  behavior of "MapMarkerToLogstashMarkerEncoder"

  it should "be a LogstashEncoder that instantiates without configuration" in {
    new MapMarkerToLogstashMarkerEncoder shouldBe a[LogstashEncoder]
  }

  // The load-bearing assertion: without the MapMarker -> LogstashMarker rewrite these keys would not be
  // top-level fields of the emitted JSON, and the dashboard indexes them as top-level fields.
  it should "lift the MapMarker entries into top-level JSON fields" in {
    val json = encode(mapMarker("jobId" -> "job-1", "executionId" -> "exec-1", "errorCode" -> "INVALID_INPUT"))
    json should include("\"jobId\":\"job-1\"")
    json should include("\"executionId\":\"exec-1\"")
    json should include("\"errorCode\":\"INVALID_INPUT\"")
  }

  it should "keep the standard log fields alongside the marker entries" in {
    val json = encode(mapMarker("jobId" -> "job-1"))
    json should include("\"level\":\"WARN\"")
    json should include("\"message\":\"Mapping failure\"")
    json should include("\"logger_name\":\"io.ignifyr.engine.data.write.SinkHandler\"")
  }

  it should "preserve the value types of the marker entries" in {
    val json = encode(mapMarker("numOfWritten" -> 12, "isStreaming" -> true))
    json should include("\"numOfWritten\":12")
    json should include("\"isStreaming\":true")
  }

  it should "encode an event with no marker at all" in {
    val json = new String(startedEncoder.encode(event(null)), StandardCharsets.UTF_8)
    json should include("\"message\":\"Mapping failure\"")
  }

  it should "leave a marker that is not a MapMarker to the delegate" in {
    val json = encode(org.slf4j.MarkerFactory.getMarker("PLAIN_MARKER"))
    json should include("\"message\":\"Mapping failure\"")
  }

  it should "encode an empty MapMarker without emitting spurious fields" in {
    val json = encode(mapMarker())
    json should include("\"message\":\"Mapping failure\"")
  }
}
