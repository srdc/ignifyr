package io.tofhir.test.engine.util

import io.tofhir.engine.util.FhirMappingUtility
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FhirMappingUtilityTest extends AnyFlatSpec with Matchers {

  "getHashedIntId" should "be deterministic across repeated calls" in {
    val hash1 = FhirMappingUtility.getHashedIntId("patient-1", 0, 999)
    val hash2 = FhirMappingUtility.getHashedIntId("patient-1", 0, 999)
    hash1 shouldBe hash2
  }

  it should "produce different values for different inputs (in general)" in {
    val hash1 = FhirMappingUtility.getHashedIntId("patient-1", 0, Int.MaxValue - 1)
    val hash2 = FhirMappingUtility.getHashedIntId("patient-2", 0, Int.MaxValue - 1)
    hash1 should not be hash2
  }

  it should "always return a value within the given inclusive range" in {
    val rangeStart = 100
    val rangeEnd = 199
    (0 until 1000).foreach { i =>
      val hashed = FhirMappingUtility.getHashedIntId(s"id-$i", rangeStart, rangeEnd)
      hashed should be >= rangeStart
      hashed should be <= rangeEnd
    }
  }

  it should "support a single-value range" in {
    val hashed = FhirMappingUtility.getHashedIntId("any-id", 5, 5)
    hashed shouldBe 5
  }

  it should "support negative ranges" in {
    val hashed = FhirMappingUtility.getHashedIntId("some-id", -50, -1)
    hashed should be >= -50
    hashed should be <= -1
  }

  it should "throw an exception when rangeEnd is less than rangeStart" in {
    an[IllegalArgumentException] should be thrownBy FhirMappingUtility.getHashedIntId("id", 10, 5)
  }

  it should "distribute hashes across the range with low collision risk" in {
    val rangeStart = 0
    val rangeEnd = 999999
    val sampleSize = 5000
    val hashes = (0 until sampleSize).map(i => FhirMappingUtility.getHashedIntId(s"unique-id-$i", rangeStart, rangeEnd))
    val distinctCount = hashes.distinct.size
    // With a range 200x the sample size, the birthday paradox predicts ~99.75% of hashes stay distinct;
    // require at least 99% to allow for statistical noise while still catching a badly-skewed hash
    distinctCount.toDouble / sampleSize should be > 0.99
  }
}