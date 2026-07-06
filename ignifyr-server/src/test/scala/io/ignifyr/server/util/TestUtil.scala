package io.ignifyr.server.util

import io.ignifyr.engine.config.IgnifyrEngineConfig
import io.ignifyr.engine.util.FileUtils
import io.ignifyr.server.repository.project.ProjectFolderRepository
import org.json4s.JArray

object TestUtil {

  /**
   * Reads the project json file as a [[JArray]]
   *
   * @param engineConfig
   * @return
   */
  def getProjectJsonFile(engineConfig: IgnifyrEngineConfig): JArray = {
    FileOperations
      .readFileIntoJson(FileUtils.getPath(ProjectFolderRepository.PROJECTS_JSON).toFile)
      .asInstanceOf[JArray]
  }
}
