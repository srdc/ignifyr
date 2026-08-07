package io.ignifyr.server.util

import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.server.model.{ResourceFilter, RowSelectionOrder}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Row selection for the "test a mapping" route: the user picks how many source rows to run the mapping
 * against and whether to take them from the start or at random. The endpoint suite only ever passes
 * "start", so the random branch — which divides by `df.count()` — is only covered here.
 */
class DataFrameUtilTest extends AnyFlatSpec with Matchers {

  private val sparkSession: SparkSession = IgnifyrConfig.sparkSession

  private def rows(n: Int): DataFrame = {
    import sparkSession.implicits._
    (1 to n).toDF("value")
  }

  "applyResourceFilter" should "take the first rows in order for the start selection" in {
    val filtered = DataFrameUtil.applyResourceFilter(rows(10), ResourceFilter(3, RowSelectionOrder.START))
    filtered.count() shouldBe 3
    filtered.collect().map(_.getInt(0)).toSeq shouldBe Seq(1, 2, 3)
  }

  it should "not fail when fewer rows exist than requested for the start selection" in {
    DataFrameUtil.applyResourceFilter(rows(2), ResourceFilter(10, RowSelectionOrder.START)).count() shouldBe 2
  }

  it should "return at most the requested number of rows for the random selection" in {
    DataFrameUtil
      .applyResourceFilter(rows(100), ResourceFilter(5, RowSelectionOrder.RANDOM))
      .count() should be <= 5L
  }

  // The random branch computes numberOfRows / df.count() as a sampling fraction, so it has to cope with
  // a request larger than the frame (fraction > 1, which Spark rejects) and with an empty frame
  // (division by zero).
  it should "clamp the sampling fraction when more rows are requested than exist" in {
    DataFrameUtil
      .applyResourceFilter(rows(3), ResourceFilter(10, RowSelectionOrder.RANDOM))
      .count() should be <= 3L
  }

  it should "return nothing for the random selection over an empty frame" in {
    val empty = rows(1).filter("value < 0")
    DataFrameUtil.applyResourceFilter(empty, ResourceFilter(5, RowSelectionOrder.RANDOM)).count() shouldBe 0
  }

  "RowSelectionOrder" should "accept only the two documented orders" in {
    RowSelectionOrder.isValid(RowSelectionOrder.START) shouldBe true
    RowSelectionOrder.isValid(RowSelectionOrder.RANDOM) shouldBe true
    RowSelectionOrder.isValid("sideways") shouldBe false
  }
}
