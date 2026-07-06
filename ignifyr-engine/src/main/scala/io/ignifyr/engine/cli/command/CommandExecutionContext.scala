package io.ignifyr.engine.cli.command

import io.ignifyr.engine.IgnifyrEngine
import io.ignifyr.engine.model.FhirMappingJob

case class CommandExecutionContext(
    ignifyrEngine: IgnifyrEngine,
    fhirMappingJob: Option[FhirMappingJob] = None,
    mappingNameUrlMap: Map[String, String] = Map.empty
) {}
