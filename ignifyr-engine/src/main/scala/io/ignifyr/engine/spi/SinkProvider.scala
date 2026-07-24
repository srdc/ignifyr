package io.ignifyr.engine.spi

import io.ignifyr.engine.data.write.BaseSinkWriter
import io.ignifyr.engine.model.SinkSettings

/**
 * Contributes a sink writer for one sink-settings type. Registered (and looked up) by
 * [[settingsClass]]; a missing provider yields a [[MissingSinkException]] at write time.
 */
trait SinkProvider {

  /** Stable identifier, e.g. "fhir-repository", "file". */
  def id: String

  /** Sink-settings model class this provider writes (the registry lookup key). */
  def settingsClass: Class[_ <: SinkSettings]

  /** Construct the writer for the given sink settings. */
  def createWriter(sinkSettings: SinkSettings): BaseSinkWriter
}
