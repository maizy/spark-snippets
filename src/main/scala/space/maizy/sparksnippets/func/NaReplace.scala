package space.maizy.sparksnippets.func

/**
 * Copyright (c) Nikita Kovaliov, maizy.ru, 2019
 * See LICENSE.txt for details.
 */

import space.maizy.sparksnippets.{ CommonFunctions, SparkDebugUtils, SparkSessionBuilder }
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object NaReplace extends SparkSessionBuilder with SparkDebugUtils {

    def main(args: Array[String]): Unit = {

        implicit val spark: SparkSession = buildSparkSession()
        implicit val sc: SparkContext = spark.sparkContext

        import spark.implicits._

        CommonFunctions.SparkWrapper.spark = spark
        CommonFunctions.SparkWrapper.sc = sc

        import CommonFunctions._

        val df = List(
            ("1-A", "Пермь"),
            ("2-B", "Москва"),
            ("3-C", "Тверь"),
            ("777-V", "Дубна")
        ).toDF("id", "city")

        val map = Map(
            "Пермь" -> "Perm",
            "Москва" -> "Moscow"
        )

        df.na.replace("city", map).show(5, truncate = false)

    }
}

