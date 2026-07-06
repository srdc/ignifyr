package io.ignifyr.engine.model.exception

final case class FhirMappingException(private val reason: String, private val cause: Throwable = None.orNull)
    extends Exception(reason: String, cause: Throwable) {}
