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
  def mul2(v: Int): Int = v * 2
  def mul2OrMinusOne(v: Integer): Int = Option(v).map(_ * 2).getOrElse(-1)
}

object IntUDFs {
  val squareUdf: UserDefinedFunction = udf { IntFunctions.square }
  val mul2Udf: UserDefinedFunction = udf { IntFunctions.mul2 _ }
  val mul2OrMinusOneUdf: UserDefinedFunction = udf { IntFunctions.mul2OrMinusOne _ }
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

    assert(IntFunctions.mul2OrMinusOne(null) == -1)

    // won't compile
    //IntFunctions.double(null)

    val variants = for (
      name <- columns;
      (postfix, func) <- ISeq(
        ("Square", squareUdf),
        ("Mul2", mul2Udf),
        ("Mul2OrMinusOne", mul2OrMinusOneUdf)
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
     |-- IntMul2: integer (nullable = true)   // if UDF doesn't allow null as a parameter it won't call for null values
     |-- IntMul2OrMinusOne: integer (nullable = false)
     |-- NullableIntSquare: integer (nullable = true)
     |-- NullableIntMul2: integer (nullable = true)
     |-- NullableIntMul2OrMinusOne: integer (nullable = false)  // null is converted explicitly by UDF. UDF always returns non null value.

    +---+-----------+---------+-------+-----------------+-----------------+---------------+-------------------------+
    |Int|NullableInt|IntSquare|IntMul2|IntMul2OrMinusOne|NullableIntSquare|NullableIntMul2|NullableIntMul2OrMinusOne|
    +---+-----------+---------+-------+-----------------+-----------------+---------------+-------------------------+
    |1  |10         |1        |2      |2                |100              |20             |20                       |
    |2  |20         |4        |4      |4                |400              |40             |40                       |
    |3  |null       |9        |6      |6                |null             |null           |-1                       |
    +---+-----------+---------+-------+-----------------+-----------------+---------------+-------------------------+

     */
  }
}
