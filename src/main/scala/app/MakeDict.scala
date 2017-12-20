package app

import core._
import data.Data
import dict.Dictionary
import fe.FE
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object MakeDict {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
          .setAppName(this.getClass.getSimpleName)
          .setMaster("local[6]")
          .set("spark.driver.maxResultSize", "2g")
          .set("spark.sql.streaming.checkpointLocation", "/home/ivica/tapklik/spark/temp")
          .set("spark.executor.memory", "4g")

        val sc = new SparkContext(conf)
        val spark: SparkSession = SparkSession
          .builder
          .appName("Stream")
          .master("local[6]")
          .getOrCreate()

        val columns = Columns.map

        val data = Data.readData(Paths.data, spark, Schemas.rawTrainingData)

        var feData = FE.feProcess(data, spark)

        val dctnry = new Dictionary(spark, sc)
        dctnry.setDict2(feData, columns)
        dctnry.save(Paths.dictPath)

        sc.stop()

    }

}

