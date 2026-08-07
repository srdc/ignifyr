package io.ignifyr.server.util

import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import io.ignifyr.engine.Execution.actorSystem
import io.ignifyr.server.model.csv.CsvHeader
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.concurrent.Future

/**
 * Covers the CSV editing behind the mapping-context and terminology `content`/`header` routes. Every
 * function here rewrites a file the user owns in place, so an off-by-one in the page arithmetic or a
 * mis-tracked column rename is silent data loss rather than an error.
 */
class CsvUtilTest extends AsyncWordSpec with Matchers {

  /** Writes the given lines to a fresh temp CSV and returns it. */
  private def csvFile(lines: String*): File = {
    val file = Files.createTempFile("ignifyr-csv-util", ".csv").toFile
    Files.write(file.toPath, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))
    file
  }

  private def linesOf(file: File): Seq[String] =
    new String(Files.readAllBytes(file.toPath), StandardCharsets.UTF_8).split("\n").map(_.trim).filter(_.nonEmpty).toSeq

  private def drain(source: Source[ByteString, Any]): Future[Seq[String]] =
    source.runWith(Sink.seq).map(_.map(_.utf8String).mkString.split("\n").map(_.trim).filter(_.nonEmpty).toSeq)

  "writeCsvHeaders" should {

    "keep the values of a renamed column" in {
      val file = csvFile("code,unit", "\"10839-9\",\"ng/ml\"")
      CsvUtil
        .writeCsvHeaders(file, Seq(CsvHeader("code", "code"), CsvHeader("targetUnit", "unit")))
        .map { _ =>
          linesOf(file) shouldBe Seq("code,targetUnit", "\"10839-9\",\"ng/ml\"")
        }
    }

    "drop the values of a removed column" in {
      val file = csvFile("a,b,c", "1,2,3")
      CsvUtil
        .writeCsvHeaders(file, Seq(CsvHeader("a", "a"), CsvHeader("c", "c")))
        .map { _ =>
          linesOf(file) shouldBe Seq("a,c", "\"1\",\"3\"")
        }
    }

    // A newly added column has no data yet, so each row gets a visible placeholder rather than an empty
    // cell — that is what tells the user in the UI which column still needs filling in.
    "fill an added column with a placeholder naming it" in {
      val file = csvFile("a", "1")
      CsvUtil
        .writeCsvHeaders(file, Seq(CsvHeader("a", "a"), CsvHeader("b", "b")))
        .map { _ =>
          linesOf(file) shouldBe Seq("a,b", "\"1\",\"<b>\"")
        }
    }

    "reorder the columns to follow the given headers" in {
      val file = csvFile("a,b", "1,2")
      CsvUtil
        .writeCsvHeaders(file, Seq(CsvHeader("b", "b"), CsvHeader("a", "a")))
        .map { _ =>
          linesOf(file) shouldBe Seq("b,a", "\"2\",\"1\"")
        }
    }

    "keep a value that contains the separator inside its quotes" in {
      val file = csvFile("code,label", "\"1\",\"a,b\"")
      CsvUtil
        .writeCsvHeaders(file, Seq(CsvHeader("code", "code"), CsvHeader("label", "label")))
        .map { _ =>
          linesOf(file) shouldBe Seq("code,label", "\"1\",\"a,b\"")
        }
    }

    "write only the header row for a file that has no data rows" in {
      val file = csvFile("a,b")
      CsvUtil.writeCsvHeaders(file, Seq(CsvHeader("a", "a"))).map(_ => linesOf(file) shouldBe Seq("a"))
    }

    // Regression: the returned Future used to complete before the write did, so a caller could observe
    // the previous content right after being told the update succeeded.
    "complete only after the file has been written" in {
      val file = csvFile("a,b", "1,2")
      CsvUtil
        .writeCsvHeaders(file, Seq(CsvHeader("renamed", "a")))
        .map(_ => linesOf(file) shouldBe Seq("renamed", "\"1\""))
    }
  }

  "getPaginatedCsvContent" should {

    "return the header plus the requested page" in {
      val file = csvFile("h", "r1", "r2", "r3", "r4", "r5")
      CsvUtil.getPaginatedCsvContent(file, pageNumber = 2, pageSize = 2).flatMap { case (source, total) =>
        total shouldBe 5
        drain(source).map(_ shouldBe Seq("h", "r3", "r4"))
      }
    }

    "return the header plus the first page" in {
      val file = csvFile("h", "r1", "r2", "r3")
      CsvUtil.getPaginatedCsvContent(file, pageNumber = 1, pageSize = 2).flatMap { case (source, _) =>
        drain(source).map(_ shouldBe Seq("h", "r1", "r2"))
      }
    }

    "return a short last page" in {
      val file = csvFile("h", "r1", "r2", "r3")
      CsvUtil.getPaginatedCsvContent(file, pageNumber = 2, pageSize = 2).flatMap { case (source, _) =>
        drain(source).map(_ shouldBe Seq("h", "r3"))
      }
    }

    "return only the header for a page past the end" in {
      val file = csvFile("h", "r1")
      CsvUtil.getPaginatedCsvContent(file, pageNumber = 5, pageSize = 2).flatMap { case (source, total) =>
        total shouldBe 1
        drain(source).map(_ shouldBe Seq("h"))
      }
    }

    "not count the header row in the total" in {
      val file = csvFile("h")
      CsvUtil.getPaginatedCsvContent(file, pageNumber = 1, pageSize = 10).map { case (_, total) => total shouldBe 0 }
    }
  }

  "writeCsvAndReturnRowNumber" should {

    "replace exactly the rows of the requested page" in {
      val file = csvFile("h", "r1", "r2", "r3", "r4")
      val replacement = Source(List(ByteString("new3"), ByteString("new4")))
      CsvUtil.writeCsvAndReturnRowNumber(file, replacement, pageNumber = 2, pageSize = 2).map { total =>
        linesOf(file) shouldBe Seq("h", "r1", "r2", "new3", "new4")
        total shouldBe 4
      }
    }

    "leave the header untouched when replacing the first page" in {
      val file = csvFile("h", "r1", "r2")
      val replacement = Source(List(ByteString("new1"), ByteString("new2")))
      CsvUtil.writeCsvAndReturnRowNumber(file, replacement, pageNumber = 1, pageSize = 2).map { total =>
        linesOf(file) shouldBe Seq("h", "new1", "new2")
        total shouldBe 2
      }
    }

    // The page is replaced in place, so a shorter replacement shrinks the file by the difference: the
    // two rows of page 2 give way to one, and the rows before the page are untouched.
    "shrink the file when the page is replaced by fewer rows" in {
      val file = csvFile("h", "r1", "r2", "r3", "r4")
      val replacement = Source(List(ByteString("only3")))
      CsvUtil.writeCsvAndReturnRowNumber(file, replacement, pageNumber = 2, pageSize = 2).map { total =>
        linesOf(file) shouldBe Seq("h", "r1", "r2", "only3")
        total shouldBe 3
      }
    }

    "report the row count excluding the header" in {
      val file = csvFile("h", "r1")
      CsvUtil
        .writeCsvAndReturnRowNumber(file, Source(List(ByteString("r1'"))), pageNumber = 1, pageSize = 1)
        .map(_ shouldBe 1)
    }
  }

  "saveFileContent" should {

    "overwrite the file with the given content" in {
      val file = csvFile("old,header", "old,row")
      CsvUtil
        .saveFileContent(file, Source(List(ByteString("new,header\n"), ByteString("new,row"))))
        .map(_ => linesOf(file) shouldBe Seq("new,header", "new,row"))
    }

    "leave no trailing remnant of a longer previous content" in {
      val file = csvFile("a,b,c,d,e,f,g,h", "1,2,3,4,5,6,7,8")
      CsvUtil
        .saveFileContent(file, Source.single(ByteString("x")))
        .map(_ => linesOf(file) shouldBe Seq("x"))
    }

    "strip carriage returns so Windows line endings do not leak into the stored file" in {
      val file = csvFile("placeholder")
      CsvUtil
        .saveFileContent(file, Source.single(ByteString("a,b\r\n1,2")))
        .map { _ =>
          new String(Files.readAllBytes(file.toPath), StandardCharsets.UTF_8) should not include "\r"
        }
    }
  }
}
