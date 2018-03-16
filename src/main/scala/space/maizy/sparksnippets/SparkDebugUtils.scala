package space.maizy.sparksnippets

/**
  * Copyright (c) Nikita Kovaliov, maizy.ru, 2018
  * See LICENSE.txt for details.
  */

import org.apache.spark.sql.DataFrame

trait SparkDebugUtils {
  implicit class DescribeSyntax(df: DataFrame) {
    def describeMe(name: String): Unit = {
      Console.println(s"=== $name ===")
      df.printSchema()
      df.show(truncate = false)
    }

    def describeMe(): Unit = describeMe("")
  }
}
