package io.ignifyr.sink.omop

import io.ignifyr.engine.spi.IgnifyrExtension

/**
 * Enterprise OMOP sink module — placeholder skeleton. Reserves the module and its ServiceLoader
 * registration for the upcoming "map to OMOP" feature: an `OmopSinkSettings`-keyed sink writer
 * typing rows against versioned OMOP CDM schemas (5.3/5.4/6.0) with FK-ordered table writes, plus
 * an OMOP-vocabulary-backed terminology service (design recorded in the edition-split plan).
 *
 * Registers nothing yet — the engine-side model classes (`OmopSinkSettings`, ...) must land in the
 * community engine (parse-everywhere rule) together with the implementation.
 */
class OmopSinkExtension extends IgnifyrExtension {

  override val id: String = "sink-omop"
}
