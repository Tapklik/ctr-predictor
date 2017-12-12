package ctr

import data.{Data, Schemas}
import dict.Dictionary
import fe.FE
import map.MapProcess
import metrics.Metrics
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object CTR {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
          .setAppName(this.getClass.getSimpleName)
          .setMaster("local[6]")
          .set("spark.driver.maxResultSize", "2g")
          .set("spark.sql.streaming.checkpointLocation", "/home/ivica/tapklik/spark/temp")
          .set("spark.executor.memory", "4g")

        val sc = new SparkContext(conf)
        var sqlContext = new SQLContext(sc)
        val spark: SparkSession = SparkSession
          .builder
          .appName("Stream")
          .master("local[6]")
          .getOrCreate()

        val trainpath = "/home/ivica/tapklik/test/5milli"
        val testpath = "/home/ivica/tapklik/test/1milli"

        val trainingColumns = Array("click", "banner_pos", "site_id", "site_domain", "site_category", "app_domain", "app_category", "device_model", "device_type",
            "device_conn_type", "C1", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21")

        val feTrainingColumns = Array("click", "banner_pos", "site_id", "site_domain", "site_category", "app_domain", "app_category", "device_model", "device_type",
            "device_conn_type", "C1", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21", "dimensions", "banner_pos_dimensions")

        val testingColumns = Array("banner_pos", "site_id", "site_domain", "site_category", "app_domain", "app_category", "device_model", "device_type",
            "device_conn_type", "C1", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21")

        val trainingData = Data.readData(trainpath, spark, Schemas.rawTrainingData)
        val testingData = Data.readData(testpath, spark, Schemas.rawTrainingData)

        var feTrainingData = FE.feProcess(trainingData, spark)
        var feTestingData = FE.feProcess(testingData, spark)

        val dctnry = new Dictionary(spark, sc)
        dctnry.load("/home/ivica/tapklik/dict/fe-test-dict-full/dict.csv")
        var mapa = dctnry.getDict()

        val mappedTrainingData = new MapProcess(feTrainingData, feTrainingColumns, mapa, true, spark)
        val mappedTestingData = new MapProcess(feTestingData, feTrainingColumns, mapa, true, spark)

        val mdl = new LogisticRegression().fit(mappedTrainingData.processedData)
        var resultDF = mdl.transform(mappedTestingData.processedData)

        val metrics = new Metrics(mdl, resultDF, spark)

        sc.stop()

    }

}

