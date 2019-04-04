package space.maizy.sparksnippets.code

/**
 * Copyright (c) Nikita Kovaliov, maizy.ru, 2019
 * See LICENSE.txt for details.
 */

import space.maizy.sparksnippets.{ CommonFunctions, SparkDebugUtils, SparkSessionBuilder }
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._


object DataframeAsValueClass extends SparkSessionBuilder with SparkDebugUtils {

    class IdAccount(val df: DataFrame) extends AnyVal {
        def appendGender(g: String): IdAccountGender = {
            new IdAccountGender(df.withColumn("Gender", lit(g)))
        }
    }

    class IdAccountGender(val df: DataFrame) extends AnyVal {
        def union(other: IdAccountGender): IdAccountGender =
            new IdAccountGender(df.union(other.df))
    }

    def main(args: Array[String]): Unit = {

        implicit val spark: SparkSession = buildSparkSession()
        implicit val sc: SparkContext = spark.sparkContext

        import spark.implicits._

        CommonFunctions.SparkWrapper.spark = spark
        CommonFunctions.SparkWrapper.sc = sc

        import CommonFunctions._

        val idAccRaw = List(("1", "b00b"), ("2", "abba")).toDF("Id", "Account")

        val idAcc = new IdAccount(idAccRaw)
        val idAccountGender1: IdAccountGender = idAcc.appendGender("male")
        val idAccountGender2: IdAccountGender = idAcc.appendGender("female")

        def filterMale(idAccGender: IdAccountGender): IdAccountGender =
            new IdAccountGender(idAccGender.df.filter($"Gender" === "male"))

        val allAccounts = idAccountGender1.union(idAccountGender2)

        allAccounts.df.show(5, truncate = false)

        // doesn't compile
        // filterMale(idAccRaw)

        filterMale(allAccounts).df.show(5, truncate = false)

    }
}
