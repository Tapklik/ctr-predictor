package fe

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf

object FE {

    val combineColumnsUDF = udf((col1:String, col2: String) =>  col1 + "x" + col2)

    def feProcess(data: DataFrame, spark: SparkSession) = {

        import spark.implicits._

        data.withColumn("dimensions", combineColumnsUDF($"C15", $"C16")).withColumn("banner_pos_dimensions", combineColumnsUDF($"banner_pos", $"dimensions"))
    }

}
