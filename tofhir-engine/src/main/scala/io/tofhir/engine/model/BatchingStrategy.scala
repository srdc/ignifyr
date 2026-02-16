package io.tofhir.engine.model

/**
 * Batching strategy for processing source data in multiple batches.
 * This allows defining custom batching logic per mapping task, such as processing data year by year,
 * ID range by ID range, or any custom grouping criteria.
 *
 * @param batchParameterSets Sequence of parameter sets, where each set represents one batch.
 *                           Each parameter set is a map of (parameterName -> parameterValue).
 *                           All parameters in a set will be substituted in preprocessSql as $parameterName.
 *
 * Example for single parameter (year):
 *   batchParameterSets = Seq(
 *     Map("year" -> "2014"),
 *     Map("year" -> "2015"),
 *     Map("year" -> "2016")
 *   )
 *
 * Example for multiple parameters (year + month):
 *   batchParameterSets = Seq(
 *     Map("year" -> "2020", "month" -> "1"),
 *     Map("year" -> "2020", "month" -> "2"),
 *     Map("year" -> "2020", "month" -> "3"),
 *     Map("year" -> "2021", "month" -> "1"),
 *     ...
 *   )
 */
case class BatchingStrategy(batchParameterSets: Seq[Map[String, String]])
