package io.ignifyr.runtime.scheduling

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeParseException

/**
 * The incremental-sync bookkeeping behind every scheduled run. Each fire reads the previous
 * synchronisation instant from `<ignifyrDbFolderPath>/scheduler/<jobId>.txt` and syncs `(lastSync, now)`;
 * afterwards it appends the new instant to that same file.
 *
 * Getting the read wrong is silent in both directions — too early a lower bound re-imports data that was
 * already mapped, too late a one skips a window forever — and the long-tier `SchedulingTest` only ever
 * exercises a single fire against a fresh directory, so none of the states below are reached there.
 */
class ScheduledTimeRangeSpec extends AnyFlatSpec with Matchers {

  private val provider = new Cron4jSchedulerProvider
  private val epoch = LocalDateTime.of(1970, 1, 1, 0, 0)

  /** A fresh, empty scheduler state directory. */
  private def schedulerFolder(): File =
    Files.createTempDirectory("ignifyr-scheduler-state").toFile

  /** Appends lines the way `runnableMappingJob` does: the instant's `toString`, one per line. */
  private def writeSyncFile(folder: File, jobId: String, instants: LocalDateTime*): Unit = {
    val writer = new FileWriter(s"${folder.toURI.getPath}/$jobId.txt", true)
    try instants.foreach(instant => writer.write(instant.toString + "\n"))
    finally writer.close()
  }

  "getScheduledTimeRange" should "start from the job's initial time when no sync has happened yet" in {
    val (from, to) = provider.getScheduledTimeRange("job-1", schedulerFolder().toURI, epoch)
    from shouldBe epoch
    to should be > from
  }

  it should "resume from the recorded synchronisation instant" in {
    val folder = schedulerFolder()
    val lastSync = LocalDateTime.of(2026, 3, 1, 12, 0)
    writeSyncFile(folder, "job-1", lastSync)

    provider.getScheduledTimeRange("job-1", folder.toURI, epoch)._1 shouldBe lastSync
  }

  // The file is append-only: every completed run adds a line, so only the last one is the current state.
  it should "take the last line of an append-only file" in {
    val folder = schedulerFolder()
    writeSyncFile(
      folder,
      "job-1",
      LocalDateTime.of(2026, 3, 1, 12, 0),
      LocalDateTime.of(2026, 3, 1, 13, 0),
      LocalDateTime.of(2026, 3, 1, 14, 0)
    )

    provider.getScheduledTimeRange("job-1", folder.toURI, epoch)._1 shouldBe LocalDateTime.of(2026, 3, 1, 14, 0)
  }

  it should "keep each job's synchronisation state separate" in {
    val folder = schedulerFolder()
    writeSyncFile(folder, "job-1", LocalDateTime.of(2026, 3, 1, 12, 0))
    writeSyncFile(folder, "job-2", LocalDateTime.of(2026, 3, 2, 12, 0))

    provider.getScheduledTimeRange("job-1", folder.toURI, epoch)._1 shouldBe LocalDateTime.of(2026, 3, 1, 12, 0)
    provider.getScheduledTimeRange("job-2", folder.toURI, epoch)._1 shouldBe LocalDateTime.of(2026, 3, 2, 12, 0)
    provider.getScheduledTimeRange("job-3", folder.toURI, epoch)._1 shouldBe epoch
  }

  it should "create the state directory when it does not exist yet" in {
    val missing = Paths.get(schedulerFolder().getAbsolutePath, "scheduler").toFile
    missing should not(exist)

    provider.getScheduledTimeRange("job-1", missing.toURI, epoch)._1 shouldBe epoch
    missing should exist
  }

  it should "end the range at the current time" in {
    val before = LocalDateTime.now()
    val (_, to) = provider.getScheduledTimeRange("job-1", schedulerFolder().toURI, epoch)
    to should be >= before
    to should be <= LocalDateTime.now()
  }

  /*
   * Pinned rather than changed: only FileNotFoundException is caught, so a sync file that exists but has
   * no parsable last line fails the run instead of falling back to `startTime`. A crash between opening
   * the writer and writing the line leaves exactly such a zero-byte file, and from then on the job stops
   * syncing. Changing that is a scheduling-semantics decision (silently re-syncing from `initialTime`
   * could re-import a lot of data), so it is recorded here rather than fixed in passing.
   */
  it should "fail rather than fall back when the sync file holds no parsable instant" in {
    val folder = schedulerFolder()
    Files.createFile(Paths.get(folder.getAbsolutePath, "job-1.txt"))

    a[DateTimeParseException] should be thrownBy provider.getScheduledTimeRange("job-1", folder.toURI, epoch)
  }
}
