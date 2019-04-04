package space.maizy.sparksnippets.agg

/**
 * Copyright (c) Nikita Kovaliov, maizy.ru, 2019
 * See LICENSE.txt for details.
 */

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import space.maizy.sparksnippets.{ SparkDebugUtils, SparkSessionBuilder }

object ArgMax extends SparkSessionBuilder with SparkDebugUtils {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = buildSparkSession()
    implicit val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    val events = List(
      ("Bob", "green", Some(100), "ok"),
      ("Bob", "red", Some(150), "error"),
      ("Mary", "yellow", Some(10), "ok"),
      ("Frank", "black", Some(1000), "ok"),
      ("Frank", "gray", Some(1), "ok"),
      ("Alice", "blue", None, "empty"),  // any of this line and line bellow may be choosed
      ("Alice", "gray", None, "empty")
    ).toDF("name", "color", "time", "status")

    val aggColumn = "name"
    val maxByColumn = "time"
    val otherColumnsName = List(maxByColumn) ++
      events.columns.filterNot(List(aggColumn, maxByColumn).contains(_)).toList
    val otherColumns = otherColumnsName.map(col)

    val lastEvents = events
      .groupBy(aggColumn)
      .agg(
        max(struct(otherColumns: _*)) as "data"
      )
      .select(col(aggColumn), $"data.*")

    lastEvents
      .show(10, truncate = false)

    /*
        +-----+----+------+------+
        |name |time|color |status|
        +-----+----+------+------+
        |Mary |10  |yellow|ok    |
        |Bob  |150 |red   |error |
        |Alice|null|gray  |empty |
        |Frank|1000|black |ok    |
        +-----+----+------+------+
     */

  }
}
