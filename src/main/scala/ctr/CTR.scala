package ctr

import data.Data
import dict.Dictionary
import fe.FE
import map.MapProcess
import metrics.Metrics
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import core._

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

        val trainpath = Paths.trainingData
        val testpath = Paths.testingData

        val trainingColumns = Columns.training

        val testingColumns = Columns.testing

        val trainingData = Data.readData(trainpath, spark, Schemas.rawTrainingData)
        val testingData = Data.readData(testpath, spark, Schemas.rawTrainingData)

        var feTrainingData = FE.feProcess(trainingData, spark)
        var feTestingData = FE.feProcess(testingData, spark)

        val dctnry = new Dictionary(spark, sc)
        dctnry.load(Paths.dictPath)
        var mapa = dctnry.getDict()

        val mappedTrainingData = new MapProcess(feTrainingData, trainingColumns, mapa, true, spark)
        val mappedTestingData = new MapProcess(feTestingData, trainingColumns, mapa, true, spark)

        val mdl = new LogisticRegression().fit(mappedTrainingData.processedData)
        var resultDF = mdl.transform(mappedTestingData.processedData)

        val metrics = new Metrics(mdl, resultDF, spark)

        sc.stop()

    }

}

