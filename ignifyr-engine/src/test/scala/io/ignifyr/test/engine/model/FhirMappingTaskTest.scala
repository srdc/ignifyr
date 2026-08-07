package io.ignifyr.test.engine.model

import io.ignifyr.engine.model.{BatchingStrategy, FhirMappingTask, SqlSource}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Covers the substitution half of the batching strategy: `batchParameterSets` are rewritten into each
 * source binding's `preprocessSql` as `$parameterName`. The orchestration half (one execution per
 * parameter set) lives in `FhirMappingJobManager` and is exercised by the SQL integration suite.
 */
class FhirMappingTaskTest extends AnyFlatSpec with Matchers {

  /** A task whose single source binding carries the given preprocess SQL. */
  private def taskWith(preprocessSql: Option[String], aliases: Seq[String] = Seq("source")): FhirMappingTask =
    FhirMappingTask(
      name = "batched-task",
      mappingRef = "http://test/mappings/batched",
      sourceBinding =
        aliases.map(alias => alias -> SqlSource(tableName = Some("visits"), preprocessSql = preprocessSql)).toMap,
      batchingStrategy = Some(BatchingStrategy(Seq(Map("year" -> "2014"))))
    )

  private def sqlOf(task: FhirMappingTask, alias: String = "source"): Option[String] =
    task.sourceBinding(alias).preprocessSql

  "substituteBatchParameters" should "substitute a single parameter" in {
    val task = taskWith(Some("SELECT * FROM visits WHERE year = $year"))
    sqlOf(task.substituteBatchParameters(Map("year" -> "2014"))) shouldBe
      Some("SELECT * FROM visits WHERE year = 2014")
  }

  it should "substitute every parameter of a multi-parameter batch" in {
    val task = taskWith(Some("SELECT * FROM visits WHERE year = $year AND month = $month"))
    sqlOf(task.substituteBatchParameters(Map("year" -> "2020", "month" -> "1"))) shouldBe
      Some("SELECT * FROM visits WHERE year = 2020 AND month = 1")
  }

  it should "substitute a parameter at every occurrence" in {
    val task = taskWith(Some("SELECT $year FROM visits WHERE year = $year"))
    sqlOf(task.substituteBatchParameters(Map("year" -> "2014"))) shouldBe
      Some("SELECT 2014 FROM visits WHERE year = 2014")
  }

  // Regression: substituting the shorter name first rewrote the prefix of the longer one, so
  // `$yearEnd` came out as `2020End`. Map iteration order does not make either order reliable.
  it should "not rewrite a longer parameter name that starts with a shorter one" in {
    val task = taskWith(Some("SELECT * FROM visits WHERE year >= $year AND year < $yearEnd"))
    sqlOf(task.substituteBatchParameters(Map("year" -> "2020", "yearEnd" -> "2021"))) shouldBe
      Some("SELECT * FROM visits WHERE year >= 2020 AND year < 2021")
  }

  it should "not rewrite the prefix regardless of the order the parameters are given in" in {
    val task = taskWith(Some("$yearEnd/$year"))
    val fromShortFirst = sqlOf(task.substituteBatchParameters(Map("year" -> "2020", "yearEnd" -> "2021")))
    val fromLongFirst = sqlOf(task.substituteBatchParameters(Map("yearEnd" -> "2021", "year" -> "2020")))
    fromShortFirst shouldBe Some("2021/2020")
    fromLongFirst shouldBe fromShortFirst
  }

  it should "substitute in every source binding of the task" in {
    val task = taskWith(Some("SELECT * FROM t WHERE year = $year"), aliases = Seq("main", "secondary"))
    val substituted = task.substituteBatchParameters(Map("year" -> "2014"))
    sqlOf(substituted, "main") shouldBe Some("SELECT * FROM t WHERE year = 2014")
    sqlOf(substituted, "secondary") shouldBe Some("SELECT * FROM t WHERE year = 2014")
  }

  it should "leave a source binding without preprocess SQL untouched" in {
    val task = taskWith(None)
    val substituted = task.substituteBatchParameters(Map("year" -> "2014"))
    sqlOf(substituted) shouldBe None
    substituted.sourceBinding.keySet shouldBe task.sourceBinding.keySet
  }

  it should "leave a placeholder with no matching parameter in place" in {
    val task = taskWith(Some("SELECT * FROM visits WHERE year = $year AND site = $site"))
    sqlOf(task.substituteBatchParameters(Map("year" -> "2014"))) shouldBe
      Some("SELECT * FROM visits WHERE year = 2014 AND site = $site")
  }

  it should "preserve everything else about the task" in {
    val task = taskWith(Some("SELECT * FROM visits WHERE year = $year"))
    val substituted = task.substituteBatchParameters(Map("year" -> "2014"))
    substituted.name shouldBe task.name
    substituted.mappingRef shouldBe task.mappingRef
    substituted.batchingStrategy shouldBe task.batchingStrategy
  }
}
