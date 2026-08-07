package io.ignifyr.test.engine.util

import io.ignifyr.engine.util.SparkUtil
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.{Files, Path, Paths}

/**
 * Covers the commit-directory bookkeeping `FileStreamInputArchiver` drives on a timer. The paths below
 * are the ones the archiver hits before Spark has written anything: a checkpoint directory that does
 * not exist yet, and one that exists but holds only Spark's own dotted metadata files.
 */
class SparkUtilTest extends AnyFlatSpec with Matchers {

  private def tempDirectory(): File = Files.createTempDirectory("ignifyr-spark-util-test").toFile

  private def touch(directory: File, name: String): Path =
    Files.createFile(Paths.get(directory.getAbsolutePath, name))

  "getLastCommitOffset" should "return the highest commit file name" in {
    val commitDirectory = tempDirectory()
    Seq("0", "1", "2", "10").foreach(touch(commitDirectory, _))
    SparkUtil.getLastCommitOffset(commitDirectory) shouldBe 10
  }

  it should "ignore Spark's dotted metadata files" in {
    val commitDirectory = tempDirectory()
    touch(commitDirectory, "0")
    touch(commitDirectory, ".0.crc")
    SparkUtil.getLastCommitOffset(commitDirectory) shouldBe 0
  }

  // -1 leaves the archiver's Range.inclusive(lastProcessed + 1, offset) empty, i.e. "nothing to archive".
  it should "return -1 for a directory holding no commit file" in {
    SparkUtil.getLastCommitOffset(tempDirectory()) shouldBe -1
  }

  it should "return -1 for a directory that does not exist" in {
    val missing = Paths.get(tempDirectory().getAbsolutePath, "no-such-checkpoint").toFile
    missing should not(exist)
    SparkUtil.getLastCommitOffset(missing) shouldBe -1
  }
}
