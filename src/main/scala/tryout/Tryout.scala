package tryout

import data.Data
import dict.Dictionary
import fe.FE
import map.MapProcess
import metrics.Metrics
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import core._


object Tryout {

    case class RawData(
                        id: String,
                        click: String,
                        hour: String,
                        C1: String,
                        banner_pos: String,
                        site_id: String,
                        site_domain: String,
                        site_category: String,
                        app_id: String,
                        app_domain: String,
                        app_category: String,
                        device_id: String,
                        device_ip: String,
                        device_model: String,
                        device_type: String,
                        device_conn_type: String,
                        C14: String,
                        C15: String,
                        C16: String,
                        C17: String,
                        C18: String,
                        C19: String,
                        C20: String,
                        C21: String
                      )

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

        /*
        val trainpath = "/home/ivica/tapklik/realrun/train"
        val testpath = "/home/ivica/tapklik/realrun/test"

        val trainingColumns = Array("click", "banner_pos", "site_id", "site_domain", "site_category", "app_domain", "app_category", "device_model", "device_type",
            "device_conn_type", "C1", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21")

        val feTrainingColumns = Array("click", "banner_pos", "site_id", "site_domain", "site_category", "app_domain", "app_category", "device_model", "device_type",
            "device_conn_type", "C1", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21", "dimensions", "banner_pos_dimensions")

        val testingColumns = Array("banner_pos", "site_id", "site_domain", "site_category", "app_domain", "app_category", "device_model", "device_type",
            "device_conn_type", "C1", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21")

        val trainingData = Data.readData(trainpath, spark, Schemas.rawTrainingData)
        val testingData = Data.readData(testpath, spark, Schemas.rawTrainingData)


        val someData = Seq(
            Row("2194394749304968771", "0", "14102200", "1005", "0", "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d9", "07d7df22", "a99f214a", "75710ded", "8a4875bd", "1", "0", "21762", "320", "50", "2502", "0", "35", "-1", "221"),
            Row("11689392121214183238", "0", "14103010", "1005", "0", "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d9", "07d7df22", "a99f214a", "c946e26a", "7abbbd5c", "1", "0", "22815", "320", "50", "2647", "2", "39", "100148", "23"),
            Row("4095751483837169483", "1", "14102107", "1005", "1", "0eb72673", "d2f72222", "f028772b", "ecad2386", "7801e8d9", "07d7df22", "a99f214a", "1a5bbbb1", "80fdc835", "1", "0", "17893", "320", "50", "2039", "2", "39", "100076", "32"),
            Row("7230586900182388074", "0", "14102816", "1005", "1", "57ef2c87", "bd6d812f", "f028772b", "ecad2386", "7801e8d9", "07d7df22", "a99f214a", "85751c07", "daa861e9", "1", "0", "19772", "320", "50", "2227", "0", "935", "100077", "48"),
            Row("4361023032757015145", "0", "14102417", "1005", "0", "85f751fd", "c4e18dd6", "50e219e0", "92f5800b", "ae637522", "0f2161f8", "a99f214a", "85804f8f", "981edffc", "1", "3", "21191", "320", "50", "2424", "1", "161", "100189", "71")
        )
        var treningdata = sqlContext.createDataFrame(sc.parallelize(someData), StructType(Schemas.rawTrainingData))

        val someData2 = Seq(
            Row("2194394749304968771", "14102200", "1005", "0", "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d9", "07d7df22", "a99f214a", "75710ded", "8a4875bd", "1", "0", "21762", "320", "50", "2502", "0", "35", "-1", "221"),
            Row("11689392121214183238", "14103010", "1005", "0", "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d9", "07d7df22", "a99f214a", "c946e26a", "7abbbd5c", "1", "0", "22815", "320", "50", "2647", "2", "39", "100148", "23"),
            Row("4095751483837169483", "14102107", "1005", "1", "0eb72673", "d2f72222", "f028772b", "ecad2386", "7801e8d9", "07d7df22", "a99f214a", "1a5bbbb1", "80fdc835", "1", "0", "17893", "320", "50", "2039", "2", "39", "100076", "32"),
            Row("7230586900182388074", "14102816", "1005", "1", "57ef2c87", "bd6d812f", "f028772b", "ecad2386", "7801e8d9", "07d7df22", "a99f214a", "85751c07", "daa861e9", "1", "0", "19772", "320", "50", "2227", "0", "935", "100077", "48"),
            Row("4361023032757015145", "14102417", "1005", "0", "85f751fd", "c4e18dd6", "50e219e0", "92f5800b", "ae637522", "0f2161f8", "a99f214a", "85804f8f", "981edffc", "1", "3", "21191", "320", "50", "2424", "1", "161", "100189", "71")
        )
        var testingdata = sqlContext.createDataFrame(sc.parallelize(someData2), StructType(Schemas.rawTestingData))


        var feTrainingData = FE.feProcess(trainingData, spark)
        var feTestingData = FE.feProcess(testingData, spark)
        */

        val dctnry = new Dictionary(spark, sc)
        dctnry.load(Paths.dictPath)
        var mapa = dctnry.getDict()

        mapa.foreach(println)


        /*

        val mappedTrainingData = new MapProcess(feTrainingData, feTrainingColumns, mapa, true, spark)
        val mappedTestingData = new MapProcess(feTestingData, feTrainingColumns, mapa, true, spark)

        mappedTrainingData.processedData.show()

        val mdl = new LogisticRegression().fit(mappedTrainingData.processedData)

        var resultDF = mdl.transform(mappedTestingData.processedData)

        val metrics = new Metrics(mdl, resultDF, spark)
        data2.printSchema()
        data2.show()
        val dctnry = new Dictionary(spark, sc)
        dctnry.load("/home/ivica/tapklik/dict/test-dict/dict.csv")
        var mapa = dctnry.getDict()

        val data2 = MapProcess.setMap(columns, data, spark, mapa)

        data2.printSchema()
        data2.show()

        var features = spark.sqlContext.emptyDataFrame
        import spark.implicits._

        features = data.map{ row =>
            var elements = List[String]()
            row.toSeq.foreach{ elem =>
                println("ušlo u drugi loop")
                elements ::= elem.toString
            }
            elements.toVector
        }.toDF("features")

        */

        sc.stop()

    }

}


