package map

import data.Data
import dict.Dictionary
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.types.StructType
import core._


class MapProcess (trData: DataFrame, columns: Array[String],
                  dict: Map[String, String], withClick: Boolean, spark: SparkSession) extends Serializable {

    private var clicks = spark.sqlContext.emptyDataFrame

    private var features = spark.sqlContext.emptyDataFrame

    val mappedData: DataFrame = MapProcess.mapData(trData, columns, dict, withClick)

    val processedData: DataFrame = MapProcess.setFeatures(columns, mappedData, spark, withClick)

    def getFeatures() = {
        features
    }

    def setClicks(data: DataFrame) = {
        clicks = data.select("click")
    }

    def getClicks() = {
        clicks
    }
}

object MapProcess {

    def mapData(data: DataFrame, columns: Array[String], dict: Map[String, String], withClick: Boolean) : DataFrame = {

        if(withClick){
            var encoder = RowEncoder(Schemas.feTrainingSchemaInt)
            data.select(columns.toSeq.map(col): _*).map{row =>
                var sumting = List[Int]()
                row.toSeq.zipWithIndex.foreach { elem =>
                    if(elem._2 != 0){
                        sumting ::= dict((elem._2 - 1).toString + "_" + elem._1.toString).toInt
                    } else {
                        sumting ::= elem._1.toString.toInt
                    }
                }
                Row.fromSeq(sumting.reverse)
            } (encoder)

        } else {
            var encoder = RowEncoder(Schemas.feTestingSchemaInt)
            data.select(columns.toSeq.map(col): _*).map{row =>
                var sumting = List[Int]()
                row.toSeq.zipWithIndex.foreach { elem =>
                    sumting ::= dict(elem._2.toString + "_" + elem._1.toString).toInt
                }
                Row.fromSeq(sumting.reverse)
            } (encoder)
        }
    }

    def setFeatures(columns: Array[String], mappedData: DataFrame, spark: SparkSession, withClick: Boolean): DataFrame = {

        if(withClick){
            val schema = new StructType().add("features", new VectorUDT())
            val assembler = new VectorAssembler().
              setInputCols(columns.drop(1)).
              setOutputCol("features")

            assembler.transform(mappedData).select("label", "features")

        } else {
            val schema = new StructType().add("features", new VectorUDT())
            val assembler = new VectorAssembler().
              setInputCols(columns).
              setOutputCol("features")

            assembler.transform(mappedData).select("features")
        }
    }




}
