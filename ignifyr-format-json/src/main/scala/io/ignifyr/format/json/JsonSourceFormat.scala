package io.ignifyr.format.json

import io.ignifyr.connector.file.format.{FileSourceFormat, FileSourceReadContext}
import io.ignifyr.engine.model.SourceContentTypes
import io.ignifyr.engine.util.SparkUtil
import org.apache.spark.sql.DataFrame

/**
 * Enterprise JSON / NDJSON file *source* format (each line of an NDJSON file is a separate JSON
 * object). Contributed to the file connector's format sub-SPI via ServiceLoader; kept out of the
 * community edition per the edition split (the community file sink still writes NDJSON).
 */
class JsonSourceFormat extends FileSourceFormat {

  override val contentTypes: Seq[String] = Seq(SourceContentTypes.JSON, SourceContentTypes.NDJSON)

  override def read(ctx: FileSourceReadContext): DataFrame = {
    val binding = ctx.mappingSourceBinding
    if (ctx.mappingJobSourceSettings.asStream) {
      // schema cannot be inferred for streaming so let's infer it ourselves
      val inferredSchema = ctx.spark.read.options(binding.options).json(ctx.finalPath).schema
      ctx.spark.readStream
        .options(binding.options)
        .schema(inferredSchema)
        .json(ctx.finalPath)
    } else if (ctx.isZipFile) {
      val unzippedFileContents = SparkUtil.readZip(ctx.finalPath, ctx.spark)
      ctx.spark.read.options(binding.options).json(unzippedFileContents)
    } else {
      ctx.spark.read.options(binding.options).json(ctx.finalPath)
    }
  }
}
