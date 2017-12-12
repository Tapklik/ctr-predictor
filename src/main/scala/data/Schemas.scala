package data

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Schemas {

    val rawTrainingData = StructType(Array(
        StructField("id", StringType, nullable = false),
        StructField("click", StringType, nullable = true),
        StructField("hour", StringType, nullable = true),
        StructField("C1", StringType, nullable = true),
        StructField("banner_pos", StringType, nullable = true),
        StructField("site_id", StringType, nullable = true),
        StructField("site_domain", StringType, nullable = true),
        StructField("site_category", StringType, nullable = true),
        StructField("app_id", StringType, nullable = true),
        StructField("app_domain", StringType, nullable = true),
        StructField("app_category", StringType, nullable = true),
        StructField("device_id", StringType, nullable = true),
        StructField("device_ip", StringType, nullable = true),
        StructField("device_model", StringType, nullable = true),
        StructField("device_type", StringType, nullable = true),
        StructField("device_conn_type", StringType, nullable = true),
        StructField("C14", StringType, nullable = true),
        StructField("C15", StringType, nullable = true),
        StructField("C16", StringType, nullable = true),
        StructField("C17", StringType, nullable = true),
        StructField("C18", StringType, nullable = true),
        StructField("C19", StringType, nullable = true),
        StructField("C20", StringType, nullable = true),
        StructField("C21", StringType, nullable = true)
    ))

    val rawTestingData = StructType(Array(
        StructField("id", StringType, nullable = false),
        StructField("hour", StringType, nullable = true),
        StructField("C1", StringType, nullable = true),
        StructField("banner_pos", StringType, nullable = true),
        StructField("site_id", StringType, nullable = true),
        StructField("site_domain", StringType, nullable = true),
        StructField("site_category", StringType, nullable = true),
        StructField("app_id", StringType, nullable = true),
        StructField("app_domain", StringType, nullable = true),
        StructField("app_category", StringType, nullable = true),
        StructField("device_id", StringType, nullable = true),
        StructField("device_ip", StringType, nullable = true),
        StructField("device_model", StringType, nullable = true),
        StructField("device_type", StringType, nullable = true),
        StructField("device_conn_type", StringType, nullable = true),
        StructField("C14", StringType, nullable = true),
        StructField("C15", StringType, nullable = true),
        StructField("C16", StringType, nullable = true),
        StructField("C17", StringType, nullable = true),
        StructField("C18", StringType, nullable = true),
        StructField("C19", StringType, nullable = true),
        StructField("C20", StringType, nullable = true),
        StructField("C21", StringType, nullable = true)
    ))

    val testingSchemaString = StructType(Array(
        StructField("banner_pos", StringType, nullable = true),
        StructField("site_id", StringType, nullable = true),
        StructField("site_domain", StringType, nullable = true),
        StructField("site_category", StringType, nullable = true),
        StructField("app_domain", StringType, nullable = true),
        StructField("app_category", StringType, nullable = true),
        StructField("device_model", StringType, nullable = true),
        StructField("device_type", StringType, nullable = true),
        StructField("device_conn_type", StringType, nullable = true),
        StructField("C1", StringType, nullable = true),
        StructField("C14", StringType, nullable = true),
        StructField("C15", StringType, nullable = true),
        StructField("C16", StringType, nullable = true),
        StructField("C17", StringType, nullable = true),
        StructField("C18", StringType, nullable = true),
        StructField("C19", StringType, nullable = true),
        StructField("C20", StringType, nullable = true),
        StructField("C21", StringType, nullable = true)
    ))

    val trainingSchemaString = StructType(Array(
        StructField("click", StringType, nullable = true),
        StructField("banner_pos", StringType, nullable = true),
        StructField("site_id", StringType, nullable = true),
        StructField("site_domain", StringType, nullable = true),
        StructField("site_category", StringType, nullable = true),
        StructField("app_domain", StringType, nullable = true),
        StructField("app_category", StringType, nullable = true),
        StructField("device_model", StringType, nullable = true),
        StructField("device_type", StringType, nullable = true),
        StructField("device_conn_type", StringType, nullable = true),
        StructField("C1", StringType, nullable = true),
        StructField("C14", StringType, nullable = true),
        StructField("C15", StringType, nullable = true),
        StructField("C16", StringType, nullable = true),
        StructField("C17", StringType, nullable = true),
        StructField("C18", StringType, nullable = true),
        StructField("C19", StringType, nullable = true),
        StructField("C20", StringType, nullable = true),
        StructField("C21", StringType, nullable = true)
    ))

    val feTrainingSchemaInt = StructType(Array(
        StructField("label", IntegerType, nullable = true),
        StructField("banner_pos", IntegerType, nullable = true),
        StructField("site_id", IntegerType, nullable = true),
        StructField("site_domain", IntegerType, nullable = true),
        StructField("site_category", IntegerType, nullable = true),
        StructField("app_domain", IntegerType, nullable = true),
        StructField("app_category", IntegerType, nullable = true),
        StructField("device_model", IntegerType, nullable = true),
        StructField("device_type", IntegerType, nullable = true),
        StructField("device_conn_type", IntegerType, nullable = true),
        StructField("C1", IntegerType, nullable = true),
        StructField("C14", IntegerType, nullable = true),
        StructField("C15", IntegerType, nullable = true),
        StructField("C16", IntegerType, nullable = true),
        StructField("C17", IntegerType, nullable = true),
        StructField("C18", IntegerType, nullable = true),
        StructField("C19", IntegerType, nullable = true),
        StructField("C20", IntegerType, nullable = true),
        StructField("C21", IntegerType, nullable = true),
        StructField("dimensions", IntegerType, nullable = true),
        StructField("banner_pos_dimensions", IntegerType, nullable = true)
    ))

    val trainingSchemaInt = StructType(Array(
        StructField("label", IntegerType, nullable = true),
        StructField("banner_pos", IntegerType, nullable = true),
        StructField("site_id", IntegerType, nullable = true),
        StructField("site_domain", IntegerType, nullable = true),
        StructField("site_category", IntegerType, nullable = true),
        StructField("app_domain", IntegerType, nullable = true),
        StructField("app_category", IntegerType, nullable = true),
        StructField("device_model", IntegerType, nullable = true),
        StructField("device_type", IntegerType, nullable = true),
        StructField("device_conn_type", IntegerType, nullable = true),
        StructField("C1", IntegerType, nullable = true),
        StructField("C14", IntegerType, nullable = true),
        StructField("C15", IntegerType, nullable = true),
        StructField("C16", IntegerType, nullable = true),
        StructField("C17", IntegerType, nullable = true),
        StructField("C18", IntegerType, nullable = true),
        StructField("C19", IntegerType, nullable = true),
        StructField("C20", IntegerType, nullable = true),
        StructField("C21", IntegerType, nullable = true)
    ))

    val testingSchemaInt = StructType(Array(
        StructField("banner_pos", IntegerType, nullable = true),
        StructField("site_id", IntegerType, nullable = true),
        StructField("site_domain", IntegerType, nullable = true),
        StructField("site_category", IntegerType, nullable = true),
        StructField("app_domain", IntegerType, nullable = true),
        StructField("app_category", IntegerType, nullable = true),
        StructField("device_model", IntegerType, nullable = true),
        StructField("device_type", IntegerType, nullable = true),
        StructField("device_conn_type", IntegerType, nullable = true),
        StructField("C1", IntegerType, nullable = true),
        StructField("C14", IntegerType, nullable = true),
        StructField("C15", IntegerType, nullable = true),
        StructField("C16", IntegerType, nullable = true),
        StructField("C17", IntegerType, nullable = true),
        StructField("C18", IntegerType, nullable = true),
        StructField("C19", IntegerType, nullable = true),
        StructField("C20", IntegerType, nullable = true),
        StructField("C21", IntegerType, nullable = true)
    ))

}
