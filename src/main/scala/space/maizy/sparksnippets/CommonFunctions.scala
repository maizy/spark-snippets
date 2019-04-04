package space.maizy.sparksnippets

/**
 * Copyright (c) Nikita Kovaliov, maizy.ru, 2019
 * See LICENSE.txt for details.
 */

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession



object CommonFunctions extends SparkSessionBuilder with SparkDebugUtils {

object SparkWrapper {
    var spark: SparkSession = _
    var sc: SparkContext = _
}

implicit lazy val spark: SparkSession = SparkWrapper.spark
implicit lazy val sc: SparkContext = SparkWrapper.sc

// for copy-pasting to zeppelin

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util._

import spark.implicits._

/* read/write data in different formats */

def readOrc(url: String): DataFrame = {
    println(s"\nread orc from $url")
    val fixedURL = if (url.last != '/') url + "/" else url
    val schema = Try(spark.read.orc(fixedURL + "part-00000*"))
            .orElse(Try(spark.read.orc(fixedURL + "part-r-00000*")))
            .orElse(Try(spark.read.orc(fixedURL)))
            .recover {
                case e: Throwable => throw new RuntimeException("Unable to read ORC schema from " + fixedURL, e)
            }
            .get.schema
    spark.read.schema(schema).orc(fixedURL)
}

def readTsv(url: String): DataFrame = {
    println(s"\nread tsv from $url")
    spark.read.option("header", "true").option("delimiter", "\t").csv(url)
}

def readCsv(url: String): DataFrame = {
    println(s"\nread csv from $url")
    spark.read.option("header", "true").csv(url)
}

def writeTsv(df: DataFrame, url: String): Unit = {
    println(s"\nsave tsv to $url")
    df.write
            .option("header", "true")
            .option("delimiter", "\t")
            .csv(url)
}

def writeOrc(df: DataFrame, url: String): Unit = {
    println(s"\nsave orc to $url")
    df.write
            .format("orc")
            .options(Map("compression" -> "zlib"))
            .save(url)
}

def readText(url: String): String = {
    sc.wholeTextFiles(url).collectAsMap().values.head
}

def readAvro(url: String): DataFrame = {
    spark.read
        .format("com.databricks.spark.avro")
        .load(url)
}


/* show data */

def showJson(text: String): Unit = {
    val randID = scala.util.Random.nextLong.toString
    println(
        s"""%html script:
          |<div id="json-$randID" style="white-space: pre;">&nbsp;</div>
          |<script>
          | var data = $text;
          | var str = JSON.stringify(data, null, 2);
          | document.getElementById("json-$randID").innerText = str;
          |</script>""".stripMargin)
}



def printSampleByColumn(df: DataFrame, col: String, limit: Int = 100): Unit = {
    println(s"Random $limit items for $col: " +
            s"${df.orderBy(rand()).limit(limit).collect().map(_.getAs[String](col)).mkString(", ")}")
}

def stage[A](stageName: String)(f: => A): A = {
    try {
        spark.sparkContext.setLocalProperty("callSite.short", stageName)
        f
    } finally {
        spark.sparkContext.setLocalProperty("callSite.short", null)
    }
}

// for using method overloading
object m {
    def showRand(df: DataFrame, numRows: Int, truncated: Boolean): Unit = df.orderBy(rand()).show(numRows, truncated)
    def showRand(df: DataFrame, truncated: Boolean): Unit = showRand(df, numRows = 20, truncated)
    def showRand(df: DataFrame, numRows: Int): Unit = showRand(df, numRows, truncated = false)
    def showRand(df: DataFrame): Unit = showRand(df, numRows = 20, truncated = false)
    def selectRand(df: DataFrame, numRows: Int): DataFrame = df.orderBy(rand()).limit(numRows)
    def selectRand(df: DataFrame): DataFrame = selectRand(df, numRows = 20)

    def getFirstString(df: DataFrame, column: String): String = {
        df.limit(1).collect.head.getAs[String](column)
    }

    def getFirstString(df: DataFrame): String = {
        val detectedColumn = df.schema.fields.filter(_.dataType == StringType).head.name
        getFirstString(df, detectedColumn)
    }
}

/* utilities */
// clean spark storage
def cleanStorage(): Unit = {
    sc.getPersistentRDDs.values.foreach(_.unpersist())
}

/* math */
def median(seq: Seq[Long]): Double = {
  val sortedSeq = seq.sortWith(_ < _)
  if (seq.size % 2 == 1) {
      sortedSeq(sortedSeq.size / 2).toDouble
  } else {
    val (up, down) = sortedSeq.splitAt(seq.size / 2)
    (up.last.toDouble + down.head) / 2
  }
}

def measureTime[T](label: String = "")(f: => T): (T, java.time.Duration) = {
    val start = java.time.ZonedDateTime.now()
    val res = f
    val end = java.time.ZonedDateTime.now()
    val duration = java.time.Duration.between(start, end)
    val formatter = java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME

    println(
        s"${if (!label.isEmpty) s"== $label ==\n" else ""}" +
            s"Start: ${start.format(formatter)}\n" +
            s"End: ${end.format(formatter)}\n" +
            s"Time: $duration (${duration.toMillis}ms)\n"
    )
    (res, duration)
}

def measureTimeWithRepeats[T](num: Int = 5, label: String = "")(f: Integer => T): Double = {
    val durations = for (
        i <- 1 to num;
        res = measureTime(s"$label: $i")(f(i))
    ) yield res._2.toMillis

    val avg = durations.sum.toDouble / num
    val durationsStr = durations.mkString(", ")
    println(s"== $label : Summary ==\n" +
            s"Repeats: $num\n" +
            s"Durations: $durationsStr\n" +
            s"Avg time: ${avg.formatted("%.2f")} ms\n" +
            s"Median time: ${median(durations)} ms")
    avg
}

}

