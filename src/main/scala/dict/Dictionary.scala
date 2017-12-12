package dict

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source


case class dictRow(index: String, uid: String)


class Dictionary(spark: SparkSession, sc: SparkContext) extends Serializable {

    private var dictionary: Map[String, String] = Map()

    def setDict(data: DataFrame, columns: Array[String]): Unit = {

        import spark.implicits._

        val indexify = udf((i:String, col: String) =>  i + "_" + col)

        var ds = spark.emptyDataset[dictRow]

        for ((column, i) <- columns.zipWithIndex) {
            ds = ds.union(
                data.select(column).distinct()
                  .withColumnRenamed(column, "value")
                  .withColumn("index", indexify(lit(i), $"value"))
                  .withColumn("uid", row_number().over(
                      Window.orderBy("index"))
                  ).select("index", "uid")
                  .as[dictRow]
            )
        }

       val tempDictionary = ds.rdd.keyBy(_.index)
          .mapValues(_.uid)
          .reduceByKey((a, b) => a)
          .collectAsMap()

        dictionary = tempDictionary.toMap
    }

    def getDict(): Map[String, String] = {
        dictionary
    }

    def load(path: String): Unit = {
        for (line <- Source.fromFile(path).getLines) {
            var key = line.toString.slice(0, line.indexOf(","))
            var value = line.toString.slice(line.indexOf(",") + 1, line.length)

            if(!dictionary.exists(_ == key -> value)){
                dictionary += (key -> value)
            }

        }
    }

    def save(path: String): Unit = {
        import spark.implicits._
        val ds = dictionary.toSeq.toDS()
        ds.coalesce(1).write.csv(path)
    }

    def print() = {
        println("====================================================================")
        println(dictionary)
        println("====================================================================")
    }
}
