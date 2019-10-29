import java.time._
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object DataFrameImplementation {
  // disable logging of INFO
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  case class RowData(DATE: String, AllNames: String)

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Lab_1").config("spark.master", "local").getOrCreate()
    import spark.implicits._

    registerSqlFunctions(spark)

    // read in files and store them as a temp SQL table
    spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .csv("data/segment/*.gkg.csv")
      .as[RowData]
      .filter(_.AllNames != null)
      .createOrReplaceTempView("GDELT")

    val result = spark.sql("""
      SELECT date, collect_list(map("topic", topic, "count", count)) AS topics
      FROM (
        SELECT dense_rank() OVER (PARTITION BY `date` ORDER BY `count` DESC) AS rank, *
        FROM (
          SELECT date, topic, COUNT(*) AS count
          FROM (
              SELECT format_date(DATE) AS date, explode(split_names(AllNames)) AS topic
              FROM GDELT
          )
          WHERE topic NOT LIKE '%ParentCategory%'
          GROUP BY date, topic
        )
        ORDER BY count DESC
      )
      WHERE rank <= 10
      GROUP BY date
      ORDER BY date ASC
    """)

    result.toJSON.foreach(x => println(x))
    spark.stop()
  }

  def registerSqlFunctions(spark: SparkSession): Unit = {
    // convert "Amsterdam,123;Deft,456" to [Amsterdam, Delft]
    spark.udf.register("split_names", (s: String) =>
      s.split(";").map(_.split(",")(0)))

    // convert 20150218230000 to 2015-02-18
    spark.udf.register("format_date", (s: String) =>
      LocalDateTime
        .parse(s, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
        .toLocalDate.toString)

  }

  val schema = StructType(
    Array(
      StructField("GKGRECORDID", StringType),
      StructField("DATE", StringType),
      StructField("SourceCollectionIdentifier", IntegerType),
      StructField("SourceCommonName", StringType),
      StructField("DocumentIdentifier", StringType),
      StructField("Counts", StringType),
      StructField("V2Counts", StringType),
      StructField("Themes", StringType),
      StructField("V2Themes", StringType),
      StructField("Locations", StringType),
      StructField("V2Locations", StringType),
      StructField("Persons", StringType),
      StructField("V2Persons", StringType),
      StructField("Organizations", StringType),
      StructField("V2Organizations", StringType),
      StructField("V2Tone", StringType),
      StructField("Dates", StringType),
      StructField("GCAM", StringType),
      StructField("SharingImage", StringType),
      StructField("RelatedImages", StringType),
      StructField("SocialImageEmbeds", StringType),
      StructField("SocialVideoEmbeds", StringType),
      StructField("Quotations", StringType),
      StructField("AllNames", StringType),
      StructField("Amounts", StringType),
      StructField("TranslationInfo", StringType),
      StructField("Extras", StringType)
    )
  )
}
