package space.maizy.sparksnippets.udf

/**
  * Copyright (c) Nikita Kovaliov, maizy.ru, 2018
  * See LICENSE.txt for details.
  */

import scala.collection.immutable.{ Seq => ISeq }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import space.maizy.sparksnippets.SparkSessionBuilder

object IntFunctions {
  // as Function1
  val square: Int => Int = v => v * v
  
  // as methods
  def double(v: Int): Int = v * 2
  def doubleOrZero(v: Integer): Int = Option(v).map(_ * 2).getOrElse(0)
}

object IntUDFs {
  val squareUdf: UserDefinedFunction = udf { IntFunctions.square }
  val doubleUdf: UserDefinedFunction = udf { IntFunctions.double _ }
  val doubleOrZeroUdf: UserDefinedFunction = udf { IntFunctions.doubleOrZero _ }
}

object IntUDFApp extends SparkSessionBuilder {
  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = buildSparkSession()
    import sparkSession.implicits._
    import IntUDFs._

    val columns = ISeq("Int", "NullableInt")
    val sampleDF = ISeq(
        (1, Some(10)),
        (2, Some(20)),
        (3, Option.empty[Int])
      )
      .toDF(columns: _*)

    sampleDF.printSchema()
    sampleDF.show(truncate = false)

    val variants = for (
      name <- columns;
      (postfix, func) <- ISeq(
        ("Square", squareUdf),
        ("Double", doubleUdf),
        ("DoubleOrZero", doubleOrZeroUdf)
      )
    ) yield (name, postfix, func)

    val processed = variants.foldLeft(sampleDF) { case (df, (name, postfix, func)) =>
      df.withColumn(name + postfix, func(col(name)))
    }
    processed.printSchema()
    processed.show(truncate = false)

    /*
    root
     |-- Int: integer (nullable = false)
     |-- NullableInt: integer (nullable = true)
     |-- IntSquare: integer (nullable = true)
     |-- IntDouble: integer (nullable = true)
     |-- IntDoubleOrZero: integer (nullable = false)   // <= !
     |-- NullableIntSquare: integer (nullable = true)
     |-- NullableIntDouble: integer (nullable = true)
     |-- NullableIntDoubleOrZero: integer (nullable = false)

    +---+-----------+---------+---------+---------------+-----------------+-----------------+-----------------------+
    |Int|NullableInt|IntSquare|IntDouble|IntDoubleOrZero|NullableIntSquare|NullableIntDouble|NullableIntDoubleOrZero|
    +---+-----------+---------+---------+---------------+-----------------+-----------------+-----------------------+
    |1  |10         |1        |2        |2              |100              |20               |20                     |
    |2  |20         |4        |4        |4              |400              |40               |40                     |
    |3  |null       |9        |6        |6              |null             |null             |0                      |
    +---+-----------+---------+---------+---------------+-----------------+-----------------+-----------------------+

     */
  }
}
