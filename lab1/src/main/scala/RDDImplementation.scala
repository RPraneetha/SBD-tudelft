import java.time._
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object RDDImplementation {
  // disable logging of INFO
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Lab_1").config("spark.master", "local").getOrCreate()

    val sc = spark.sparkContext

    val articleData = sc.textFile("./data/segment/*.gkg.csv")
      .map(_.split("\t"))
      .filter(_.length > 23) // filter out rows that don´t have allNames
      .map(col => (LocalDateTime.parse(col(1), DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).toLocalDate, //parse date
      col(23).split(",[0-9;]+"))) //split topic names on ;
      .flatMap { case (date, allNames) => allNames.map(name => ((date, name), 1)) }

    val distinctNames = articleData.reduceByKey(_ + _)
      .map { case ((date, topic), count) => (date, (topic, count)) }
      .groupByKey()

    // filter out values containing ParentCategory (Type ParentCategory and CategoryType ParentCategory)
    val topNames = distinctNames.mapValues(
      _.toList.filter(!_._1.contains("ParentCategory"))
        .sortBy(-_._2)
        .take(10))

    topNames.foreach(println)

    spark.stop()
  }

}
