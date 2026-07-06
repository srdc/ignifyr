package io.ignifyr.engine.mapping.job

import it.sauronsoftware.cron4j.Scheduler

import java.net.URI
import java.nio.file.Paths

/**
 * Holds a Scheduler and a URI to a folder in which latest synchronization times are kept. That folders acts
 * as a database.
 *
 * @param scheduler
 * @param folderUri
 */
case class MappingJobScheduler(scheduler: Scheduler, folderUri: URI)

object MappingJobScheduler {

  /**
   * Creates an instance of MappingJobScheduler with the provided folder path.
   *
   * @param ignifyrDbFolderPath The folder path for database
   * @return An instance of MappingJobScheduler.
   * @throws IllegalArgumentException if ignifyrDbFolderPath is empty.
   */
  def instance(ignifyrDbFolderPath: String): MappingJobScheduler = {
    if (ignifyrDbFolderPath.isEmpty) {
      throw new IllegalArgumentException(
        "runJob is called with a scheduled mapping job, but ignifyr.db is not configured."
      );
    }
    MappingJobScheduler(new Scheduler(), Paths.get(ignifyrDbFolderPath, "scheduler").toUri)
  }
}
