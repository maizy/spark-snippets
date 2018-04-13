package space.maizy.sparksnippets.bugs

/**
 * Copyright (c) Nikita Kovaliov, maizy.ru, 2018
 * See LICENSE.txt for details.
 */

import org.apache.spark.sql.{ DataFrame, SaveMode, SparkSession }
import space.maizy.sparksnippets.SparkSessionBuilder
import scala.collection.immutable

object LazyShow extends SparkSessionBuilder {

  def main(args: Array[String]): Unit = {
    implicit val ss: SparkSession = buildSparkSession()
    import ss.implicits._

    val storage = "/tmp/storage"
    val baseColumns = immutable.Seq("ID", "Mod5", "Fruit")
    def generateDF(prefix: String): DataFrame = {
      val columns = baseColumns.map(c => s"${prefix}_$c")
      (1 to 10000)
          .map { i =>
            val label = i match {
              case n if n % 9 == 0 => "banana"
              case n if n % 3 == 0 => "apple"
              case _ => "NA"
            }
            (i, i % 5, label)
          }
          .toList
          .toDF(columns: _*)
          .repartition(100)
    }

    val left = generateDF("l")
    val right = generateDF("r")

    val joinExpr = baseColumns.map(c => left(s"l_$c") === right(s"r_$c")).reduce(_ && _)
    println(s"join expression: $joinExpr")

    val merged = left.join(right, joinExpr, "outer")
    println("merged")
    merged.show()

    /**
     * output for spark 2.2.0 (unexpected null in l_* columns)
     *
        +----+------+-------+----+------+-------+
        |l_ID|l_Mod5|l_Fruit|r_ID|r_Mod5|r_Fruit|
        +----+------+-------+----+------+-------+
        | 254|     4|     NA| 254|     4|     NA|
        | 423|     3| banana| 423|     3| banana|
        | 654|     4|  apple| 654|     4|  apple|
        | 735|     0|  apple| 735|     0|  apple|
        | 905|     0|     NA| 905|     0|     NA|
        | 952|     2|     NA| 952|     2|     NA|
        |1031|     1|     NA|1031|     1|     NA|
        |1172|     2|     NA|1172|     2|     NA|
        |1254|     4|  apple|1254|     4|  apple|
        |1477|     2|     NA|1477|     2|     NA|
        |1811|     1|     NA|1811|     1|     NA|
        |null|  null|   null|2392|     2|     NA|
        |null|  null|   null|2729|     4|     NA|
        |null|  null|   null|2809|     4|     NA|
        |null|  null|   null|2849|     4|     NA|
        |null|  null|   null|2967|     2|  apple|
        |null|  null|   null|3116|     1|     NA|
        |null|  null|   null|3243|     3|  apple|
        |null|  null|   null|3526|     1|     NA|
        |null|  null|   null|3595|     0|     NA|
        +----+------+-------+----+------+-------+
        only showing top 20 rows
     *
     */

    /** output for spart 2.2.1+ (expected result)
     *
      +----+------+-------+----+------+-------+
      |l_ID|l_Mod5|l_Fruit|r_ID|r_Mod5|r_Fruit|
      +----+------+-------+----+------+-------+
      | 254|     4|     NA| 254|     4|     NA|
      | 423|     3| banana| 423|     3| banana|
      | 654|     4|  apple| 654|     4|  apple|
      | 735|     0|  apple| 735|     0|  apple|
      | 905|     0|     NA| 905|     0|     NA|
      | 952|     2|     NA| 952|     2|     NA|
      |1031|     1|     NA|1031|     1|     NA|
      |1172|     2|     NA|1172|     2|     NA|
      |1254|     4|  apple|1254|     4|  apple|
      |1477|     2|     NA|1477|     2|     NA|
      |1811|     1|     NA|1811|     1|     NA|
      |2392|     2|     NA|2392|     2|     NA|
      |2729|     4|     NA|2729|     4|     NA|
      |2809|     4|     NA|2809|     4|     NA|
      |2849|     4|     NA|2849|     4|     NA|
      |2967|     2|  apple|2967|     2|  apple|
      |3116|     1|     NA|3116|     1|     NA|
      |3243|     3|  apple|3243|     3|  apple|
      |3526|     1|     NA|3526|     1|     NA|
      |3595|     0|     NA|3595|     0|     NA|
      +----+------+-------+----+------+-------+
     */

    // affects only lazy DataFrame.show()
    merged.write.mode(SaveMode.Overwrite).orc(storage)

    println("reread")
    val reread = ss.read.orc(storage)
    reread.show()
  }
}
