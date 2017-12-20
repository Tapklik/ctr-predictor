package app

import core.{Columns, Paths, Schemas}
import dict.Dictionary
import fe.FE
import map.MapProcess
import model.{LineModel, Model}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

object Predict {

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

        val someData = Seq(
            Row("2194394749304968771", "14102200", "1005", "0", "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d9", "07d7df22", "a99f214a", "75710ded", "8a4875bd", "1", "0", "21762", "320", "50", "2502", "0", "35", "-1", "221")
        )

        var columns = Columns.testing
        var line = spark.sqlContext.createDataFrame(sc.parallelize(someData), Schemas.rawTestingData)
        var feTrainingData = FE.feProcess(line, spark)
        val mappedTrainingData = new MapProcess(feTrainingData, columns, mapa, false, spark)

        val model = new LineModel(modelSize, mappedTrainingData.mappedData)

        sc.stop()

    }
}
