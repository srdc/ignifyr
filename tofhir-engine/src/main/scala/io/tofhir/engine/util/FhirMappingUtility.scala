package io.tofhir.engine.util

import com.google.common.hash.Hashing
import org.apache.commons.codec.binary.StringUtils

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

object FhirMappingUtility {
  private final val hashSeed = 0x37dd86e4

  /**
   * Create a hashed identifier for given FHIR resource type and external id
   *
   * @param resourceType
   * @param id
   * @return
   */
  def getHashedId(resourceType: String, id: String): String = {
    val inputStr = resourceType + "/" + id
    val hashCode = Hashing.murmur3_128(hashSeed).hashBytes(StringUtils.getBytesUtf8(inputStr))
    hashCode.toString
  }

  def getHashedReference(resourceType: String, id: String): String =
    s"$resourceType/${getHashedId(resourceType, id)}"

  /**
   * Deterministically map the given string id to an integer within the inclusive range [rangeStart, rangeEnd].
   * The same input id always maps to the same integer. The mapping relies on SHA-256, which provides a strong
   * avalanche effect (small input changes spread evenly across output bits), so the resulting values are spread
   * uniformly over the given range. Note that, as with any hash function, mapping into a range smaller than the
   * input domain cannot guarantee collision-free uniqueness (pigeonhole principle) -- callers targeting a range
   * with many ids should size the range generously
   *
   * @param id         Unique string id to be hashed
   * @param rangeStart Inclusive lower bound of the target integer range
   * @param rangeEnd   Inclusive upper bound of the target integer range
   * @return an integer within [rangeStart, rangeEnd]
   */
  def getHashedIntId(id: String, rangeStart: Int, rangeEnd: Int): Int = {
    require(rangeEnd >= rangeStart, s"Invalid range for getHashedIntId: rangeEnd ($rangeEnd) must be greater than or equal to rangeStart ($rangeStart)!")
    val rangeSize: Long = rangeEnd.toLong - rangeStart.toLong + 1L
    val digest = MessageDigest.getInstance("SHA-256").digest(id.getBytes(StandardCharsets.UTF_8))
    val hashLong = ByteBuffer.wrap(digest, 0, java.lang.Long.BYTES).getLong
    rangeStart + Math.floorMod(hashLong, rangeSize).toInt
  }
}
