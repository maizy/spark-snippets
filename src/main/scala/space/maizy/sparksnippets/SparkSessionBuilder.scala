package space.maizy.sparksnippets

/**
  * Copyright (c) Nikita Kovaliov, maizy.ru, 2018
  * See LICENSE.txt for details.
  */

import org.apache.spark.sql.SparkSession

trait SparkSessionBuilder {
  self =>
  def buildSparkSession(): SparkSession = {

    // TODO: spark-sumbit support
    val sparkSessionBuilder = SparkSession.builder()
      .appName(self.getClass.getSimpleName)
      .config("spark.sql.session.timeZone", "Europe/Moscow")
      .master("local[*]")
      .config("spark.driver.host", "localhost")

    sparkSessionBuilder.getOrCreate()
  }
}
