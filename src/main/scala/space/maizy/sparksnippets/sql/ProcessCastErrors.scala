package space.maizy.sparksnippets.sql

/**
 * Copyright (c) Nikita Kovaliov, maizy.ru, 2018
 * See LICENSE.txt for details.
 */

import scala.collection.immutable.{ Seq => ISeq }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes.LongType
import space.maizy.sparksnippets.{ SparkDebugUtils, SparkSessionBuilder }

object ProcessCastErrors extends SparkSessionBuilder with SparkDebugUtils {

  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = buildSparkSession()
    import sparkSession.implicits._

    val sample = ISeq(
      (Some(1L),  Some("1")),
      (Some(2L),  Some("26.8")),
      (None,      Some("n/a")),
      (None,      Some("-")),
      (None,      Some("")),
      (None,      Some(" ")),
      (None,      None)
    ).toDF("expected", "source")

    sample.describeMe()
    /*
      +--------+------+
      |expected|source|
      +--------+------+
      |1       |1     |
      |2       |2     |
      |null    |n/a   |
      |null    |-     |
      |null    |      |
      |null    |      |
      |null    |null  |
      +--------+------+

      root
       |-- expected: long (nullable = true)
       |-- source: string (nullable = true)
     */

    val casted = sample.withColumn("casted", col("source").cast(LongType))
    casted.describeMe("joined")

    /*

      +--------+------+------+
      |expected|source|casted|
      +--------+------+------+
      |1       |1     |1     |
      |2       |2     |2     |
      |null    |n/a   |null  |
      |null    |-     |null  |
      |null    |      |null  |
      |null    |      |null  |
      |null    |null  |null  |
      +--------+------+------+

      root
       |-- expected: long (nullable = true)
       |-- source: string (nullable = true)
       |-- casted: long (nullable = true)

     */
  }
}

