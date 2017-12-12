package dict

import data.{Data, Schemas}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import fe.FE

object Dict {

    case class RawData(
                     id: String,
                     click: Int,
                     hour: Int,
                     C1: Int,
                     banner_pos: Int,
                     site_id: String,
                     site_domain: String,
                     site_category: String,
                     app_id: String,
                     app_domain: String,
                     app_category: String,
                     device_id: String,
                     device_ip: String,
                     device_model: String,
                     device_type: Int,
                     device_conn_type: Int,
                     C14: Int,
                     C15: Int,
                     C16: Int,
                     C17: Int,
                     C18: Int,
                     C19: Int,
                     C20: Int,
                     C21: Int
                   )

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
          .setAppName(this.getClass.getSimpleName)
          .setMaster("local")
          .set("spark.driver.maxResultSize", "2g")
          .set("spark.sql.streaming.checkpointLocation", "/tmp/spark")

        val sc = new SparkContext(conf)
        val spark: SparkSession = SparkSession
          .builder
          .appName("Stream")
          .master("local[*]")
          .getOrCreate()

        val path = "/home/ivica/tapklik/train/"

        val columns = Array("banner_pos", "site_id", "site_domain", "site_category", "app_domain", "app_category", "device_model", "device_type",
            "device_conn_type", "C1", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21", "dimensions", "banner_pos_dimensions")

        val data = Data.readData(path, spark, Schemas.rawTrainingData)

        val feData = FE.feProcess(data, spark)

        val dict = new Dictionary(spark, sc)
        dict.setDict(feData, columns)
        dict.save("/home/ivica/tapklik/dict/fe-test-dict-full")

        dict.print()

        sc.stop()

    }

}

