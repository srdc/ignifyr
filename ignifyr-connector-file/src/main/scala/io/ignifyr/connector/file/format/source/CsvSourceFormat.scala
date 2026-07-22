package io.ignifyr.connector.file.format.source

import io.ignifyr.connector.file.format.{FileSourceFormat, FileSourceReadContext}
import io.ignifyr.engine.model.SourceContentTypes
import io.ignifyr.engine.util.SparkUtil
import org.apache.spark.sql.DataFrame

/**
 * Community CSV/TSV file source format. TSV defaults the separator to a tab unless overridden.
 */
class CsvSourceFormat extends FileSourceFormat {

  override val contentTypes: Seq[String] = Seq(SourceContentTypes.CSV, SourceContentTypes.TSV)

  override def read(ctx: FileSourceReadContext): DataFrame = {
    val binding = ctx.mappingSourceBinding
    val updatedOptions = binding.contentType match {
      case SourceContentTypes.TSV =>
        // If the file format is tsv, use tab (\t) as separator by default if it is not set explicitly
        binding.options + ("sep" -> binding.options.getOrElse("sep", "\\t"))
      case _ => binding.options
    }

    // An explicit "inferSchema" option (if provided) takes precedence over the default heuristic, so a
    // source can opt out of Spark's (sometimes wrong) type inference - e.g. when preprocessSql is just a
    // row filter and the provided schema already describes the raw file.
    val inferSchema = binding.options
      .get("inferSchema")
      .map(_.toBoolean)
      .getOrElse(ctx.schema.isEmpty || binding.preprocessSql.isDefined)
    // Give Spark the schema only when we are not inferring; otherwise let it infer the types.
    val csvSchema = if (inferSchema) None else ctx.schema
    val includeHeader = binding.options.get("header").forall(_ == "true")
    // Other options except header, inferSchema and enforceSchema
    val otherOptions =
      updatedOptions.filterNot(o => o._1 == "header" || o._1 == "inferSchema" || o._1 == "enforceSchema")

    if (ctx.mappingJobSourceSettings.asStream) {
      ctx.spark.readStream
        .option(
          "enforceSchema",
          false
        ) // Enforce schema should be false (See https://spark.apache.org/docs/latest/sql-data-sources-csv.html)
        .option("header", includeHeader)
        .option("inferSchema", inferSchema)
        .options(otherOptions)
        .schema(csvSchema.orNull)
        .csv(ctx.finalPath)
    } else if (ctx.isZipFile) {
      val unzippedFileContents = SparkUtil.readZip(ctx.finalPath, ctx.spark)
      ctx.spark.read
        .option("enforceSchema", false) // Enforce schema should be false
        .option("header", includeHeader)
        .option("inferSchema", inferSchema)
        .options(otherOptions)
        .schema(csvSchema.orNull)
        .csv(unzippedFileContents)
    } else {
      ctx.spark.read
        .option("enforceSchema", false) // Enforce schema should be false
        .option("header", includeHeader)
        .option("inferSchema", inferSchema)
        .options(otherOptions)
        .schema(csvSchema.orNull)
        .csv(ctx.finalPath)
    }
  }
}
