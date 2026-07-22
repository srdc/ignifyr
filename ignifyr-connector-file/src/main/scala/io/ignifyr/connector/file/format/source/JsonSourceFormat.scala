package io.ignifyr.connector.file.format.source

import io.ignifyr.connector.file.format.{FileSourceFormat, FileSourceReadContext}
import io.ignifyr.engine.model.SourceContentTypes
import io.ignifyr.engine.util.SparkUtil
import org.apache.spark.sql.DataFrame

/**
 * JSON / NDJSON file source format. Each line of an NDJSON file is a separate JSON object.
 *
 * NOTE: this handler is extracted into the enterprise `ignifyr-format-json` module in a later step;
 * it lives here only while the file connector is first carved out of the engine.
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
