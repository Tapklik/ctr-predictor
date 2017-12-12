package data

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Data extends Serializable {

    def readData(path : String, spark : SparkSession, schema: StructType) : DataFrame = {
        spark.read.format("com.databricks.spark.csv").option("header", "true").schema(schema).load(path).cache()
    }

}