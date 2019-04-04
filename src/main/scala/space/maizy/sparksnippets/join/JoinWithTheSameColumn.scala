package space.maizy.sparksnippets.join

/**
 * Copyright (c) Nikita Kovaliov, maizy.ru, 2019
 * See LICENSE.txt for details.
 */
import org.apache.spark.sql.SparkSession
import space.maizy.sparksnippets.{ SparkDebugUtils, SparkSessionBuilder }

object JoinWithTheSameColumn extends SparkSessionBuilder with SparkDebugUtils {

  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = buildSparkSession()
    import sparkSession.implicits._

    val sampleDF = List(
      (1, "A", "first"),
      (2, "B", "secound"),
      (3, null, "third")
    ).toDF("Id", "Letter", "Name")

    sampleDF.describeMe("sampleDF1")

    val sampleDF2 = List(
      (3, "C"),
      (4, "D")
    ).toDF("Id", "Letter")

    sampleDF2.describeMe("sampleDF2")

    val joined = sampleDF.join(
      sampleDF2,
      List("Id"),
      "left"
    )

    joined.describeMe("joined")

    // import org.apache.spark.sql.functions.col

    //joined.select(col("Letter")).describeMe("select letter")
    // => Exception: org.apache.spark.sql.AnalysisException: Reference 'Letter' is ambiguous, could be: Letter, Letter.
  }
}
