package io.ignifyr.connector.file.format.source

import io.ignifyr.connector.file.format.{FileSourceFormat, FileSourceReadContext}
import io.ignifyr.engine.model.SourceContentTypes
import org.apache.spark.sql.DataFrame

/**
 * Community Parquet file source format. (No zip handling - matching the original reader.)
 */
class ParquetSourceFormat extends FileSourceFormat {

  override val contentTypes: Seq[String] = Seq(SourceContentTypes.PARQUET)

  override def read(ctx: FileSourceReadContext): DataFrame = {
    if (ctx.mappingJobSourceSettings.asStream) {
      ctx.spark.readStream
        .options(ctx.mappingSourceBinding.options)
        .schema(ctx.schema.orNull)
        .parquet(ctx.finalPath)
    } else {
      ctx.spark.read.options(ctx.mappingSourceBinding.options).schema(ctx.schema.orNull).parquet(ctx.finalPath)
    }
  }
}
