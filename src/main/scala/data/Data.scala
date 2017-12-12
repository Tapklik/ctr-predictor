package data

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Data extends Serializable {

    val trainSchema = StructType(Array(
        StructField("id", StringType, nullable = false),
        StructField("click", IntegerType, nullable = true),
        StructField("hour", IntegerType, nullable = true),
        StructField("C1", IntegerType, nullable = true),
        StructField("banner_pos", IntegerType, nullable = true),
        StructField("site_id", StringType, nullable = true),
        StructField("site_domain", StringType, nullable = true),
        StructField("site_category", StringType, nullable = true),
        StructField("app_id", StringType, nullable = true),
        StructField("app_domain", StringType, nullable = true),
        StructField("app_category", StringType, nullable = true),
        StructField("device_id", StringType, nullable = true),
        StructField("device_ip", StringType, nullable = true),
        StructField("device_model", StringType, nullable = true),
        StructField("device_type", IntegerType, nullable = true),
        StructField("device_conn_type", IntegerType, nullable = true),
        StructField("C14", IntegerType, nullable = true),
        StructField("C15", IntegerType, nullable = true),
        StructField("C16", IntegerType, nullable = true),
        StructField("C17", IntegerType, nullable = true),
        StructField("C18", IntegerType, nullable = true),
        StructField("C19", IntegerType, nullable = true),
        StructField("C20", IntegerType, nullable = true),
        StructField("C21", IntegerType, nullable = true)
    ))

    def readData(path : String, spark : SparkSession, schema: StructType) : DataFrame = {
        spark.read.format("com.databricks.spark.csv").option("header", "true").schema(schema).load(path).cache()
    }

}