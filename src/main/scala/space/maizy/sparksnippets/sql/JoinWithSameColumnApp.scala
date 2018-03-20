package space.maizy.sparksnippets.sql

/**
 * Copyright (c) Nikita Kovaliov, maizy.ru, 2018
 * See LICENSE.txt for details.
 */

import scala.collection.immutable.{ Seq => ISeq }
import org.apache.spark.sql.SparkSession
import space.maizy.sparksnippets.{ SparkDebugUtils, SparkSessionBuilder }

object JoinWithSameColumnApp extends SparkSessionBuilder with SparkDebugUtils {

  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = buildSparkSession()
    import sparkSession.implicits._

    val sampleDF = ISeq(
      (1, "A", "first"),
      (2, "B", "secound"),
      (3, null, "third")
    )
      .toDF("Id", "Letter", "Name")

    sampleDF.describeMe("sampleDF1")

    val sampleDF2 = ISeq(
      (3, "C"),
      (4, "D")
    )
      .toDF("Id", "Letter")

    sampleDF2.describeMe("sampleDF2")

    val joined = sampleDF.join(
      sampleDF2,
      ISeq("Id"),
      "left"
    )

    joined.describeMe("joined")

    // import org.apache.spark.sql.functions.col

    //joined.select(col("Letter")).describeMe("select letter")
    // => Exception: org.apache.spark.sql.AnalysisException: Reference 'Letter' is ambiguous, could be: Letter, Letter.
  }
}
