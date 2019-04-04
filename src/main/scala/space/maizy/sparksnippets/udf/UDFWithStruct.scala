package space.maizy.sparksnippets.udf

import space.maizy.sparksnippets.{ CommonFunctions, SparkDebugUtils, SparkSessionBuilder }
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object UDFWithStruct extends SparkSessionBuilder with SparkDebugUtils {

    def main(args: Array[String]): Unit = {

        implicit val spark: SparkSession = buildSparkSession()
        implicit val sc: SparkContext = spark.sparkContext

        import spark.implicits._

        CommonFunctions.SparkWrapper.spark = spark
        CommonFunctions.SparkWrapper.sc = sc

        import CommonFunctions._

        import org.apache.spark.sql.functions._
        import org.apache.spark.sql.types._

        val resSchema = StructType(List(
            StructField("raw", StringType, nullable = true),
            StructField("size", IntegerType, nullable = false)
        ))


        val df = List("abc", "def").toDF("col")

        df.printSchema()
        df.show(false)

        /*
            +---+
            |col|
            +---+
            |abc|
            |def|
            +---+

         */

        val udfBody: String => (String, Int) = (input: String) => {
            if (input == null) {
                (null, 0)
            } else {
                (input, input.length)
            }
        }

        val mapUdf = udf(udfBody, resSchema)

        val resDf = df.withColumn("col_transformed", mapUdf($"col"))
        resDf.show(false)
        resDf.printSchema()

        /*
            +---+---------------+
            |col|col_transformed|
            +---+---------------+
            |abc|[abc, 3]       |
            |def|[def, 3]       |
            +---+---------------+

            root
             |-- col: string (nullable = true)
             |-- col_transformed: struct (nullable = true)
             |    |-- raw: string (nullable = true)
             |    |-- size: integer (nullable = false)

         */
    }
}
