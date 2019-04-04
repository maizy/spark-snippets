package space.maizy.sparksnippets.agg

/**
 * Copyright (c) Nikita Kovaliov, maizy.ru, 2019
 * See LICENSE.txt for details.
 */

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import space.maizy.sparksnippets.{ CommonFunctions, SparkDebugUtils, SparkSessionBuilder }


object AggWithNull extends SparkSessionBuilder with SparkDebugUtils {

    def main(args: Array[String]): Unit = {

        implicit val spark: SparkSession = buildSparkSession()
        implicit val sc: SparkContext = spark.sparkContext

        import spark.implicits._

        CommonFunctions.SparkWrapper.spark = spark
        CommonFunctions.SparkWrapper.sc = sc

        import CommonFunctions._

        val df = List(
            ("1", null: Integer),
            ("2", 1: Integer),
            ("2", 3: Integer)
        ).toDF("id", "count")

        df.groupBy("id").agg(max($"count")).show

        /*
            +---+----------+
            | id|max(count)|
            +---+----------+
            |  1|      null|
            |  2|         3|
            +---+----------+
         */

    }
}

