package app

import core.{Columns, Paths, Schemas}
import data.Data
import dict.Dictionary
import fe.FE
import map.MapProcess
import model.Model
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object RunModel {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
          .setAppName(this.getClass.getSimpleName)
          .setMaster("local[6]")
          .set("spark.driver.maxResultSize", "2g")
          .set("spark.sql.streaming.checkpointLocation", "/home/ivica/tapklik/spark/temp")
          .set("spark.executor.memory", "4g")
          .set("spark.driver.memory", "8g")

        val sc = new SparkContext(conf)
        var sqlContext = new SQLContext(sc)
        val spark: SparkSession = SparkSession
          .builder
          .appName("Stream")
          .master("local[6]")
          .getOrCreate()

        val dctnry = new Dictionary(spark, sc)
        dctnry.load(Paths.dictPath)
        var mapa = dctnry.getDict()
        var modelSize = mapa.size

        val trainpath = Paths.trainingData
        val trainingColumns = Columns.training
        val trainingData = Data.readData(trainpath, spark, Schemas.rawTrainingData)
        var feTrainingData = FE.feProcess(trainingData, spark)
        val mappedTrainingData = new MapProcess(feTrainingData, trainingColumns, mapa, true, spark)

        val testpath = Paths.testingData
        val testingData = Data.readData(testpath, spark, Schemas.rawTrainingData)
        var feTestingData = FE.feProcess(testingData, spark)
        val mappedTestingData = new MapProcess(feTestingData, trainingColumns, mapa, true, spark)

        val model = new Model(modelSize, false, false, mappedTrainingData.mappedData, mappedTestingData.mappedData, trainingColumns)

        sc.stop()

    }

}
