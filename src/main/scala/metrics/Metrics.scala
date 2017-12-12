package metrics

import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegressionModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.avg

class Metrics(model: LogisticRegressionModel, resultDF: DataFrame, spark: SparkSession) {

    private val trainingSummary = model.summary

    private val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    val aUC : Double = Metrics.getAUC(binarySummary)
    val precision : DataFrame = Metrics.getPrecision(binarySummary)
    val logLossDF: DataFrame = Metrics.getLogLoss(resultDF, spark)
    val confusionMatrix : DataFrame = Metrics.getConfusionMatrix(resultDF, spark)
    val accuracy : DataFrame = Metrics.getAccuracy(resultDF, spark)


    Metrics.printMetrics(aUC, precision, logLossDF, confusionMatrix, accuracy)

}

object Metrics {

    def getAccuracy(resultDF: DataFrame, spark: SparkSession) = {
        import spark.implicits._

        resultDF.select("label", "prediction").map{ row =>

            var label = row(0).toString.toDouble
            var prediction = row(1).toString.toDouble

            var hit = 0

            if (label == prediction){
                hit = 1
            }
            else{
                hit = 0
            }
            hit
        }.toDF("accuracy").select(avg($"accuracy"))
    }

    def getAUC(summary: BinaryLogisticRegressionSummary) = {
        summary.areaUnderROC
    }

    def getConfusionMatrix(resultDF: DataFrame, spark: SparkSession) = {

        import spark.implicits._

        resultDF.select("label", "prediction").map{ row =>
            var label = row(0).toString.toDouble
            var prediction = row(1).toString.toDouble

            var state = "p"

            if (label == 0.0){
                if(prediction == 0.0){
                    state = "True Negative"
                }
                else {
                    state = "False Positive"
                }
            }
            else{
                if(prediction == 0.0){
                    state = "False Negative"
                }
                else {
                    state = "True Positive"
                }
            }
            state
        }.toDF("state").groupBy("state").count()
    }

    def getLogLoss(resultDF: DataFrame, spark: SparkSession) : DataFrame = {

        import spark.implicits._

        resultDF.select("label", "probability").map{ row =>
            var probability_1 = row(1).toString.slice(row(1).toString.indexOf(",") + 1, row(1).toString.length - 1).toDouble
            var probability_0 = 1 - probability_1
            var label = row(0).toString.toDouble
            var sum = 0.0

            if (label == 0.0){
                var y_0 = 1
                var y_1 = 0
                var r_0 = 0.0
                var r_1 = 0.0
                r_0 = y_0 * math.log(math.max(math.min(probability_0, 1 - 10E-15), 10E-15))
                r_1 = y_1 * math.log(math.max(math.min(probability_1, 1 - 10E-15), 10E-15))
                sum = r_0 + r_1
            }
            else{
                var y_0 = 0
                var y_1 = 1
                var r_0 = 0.0
                var r_1 = 0.0
                r_0 = y_0 * math.log(math.max(math.min(probability_0, 1 - 10E-15), 10E-15))
                r_1 = y_1 * math.log(math.max(math.min(probability_1, 1 - 10E-15), 10E-15))
                sum = r_0 + r_1
            }

            -1 * sum
        }.toDF("logloss").select(avg($"logloss"))
    }

    def getPrecision(summary: BinaryLogisticRegressionSummary): DataFrame = {
        summary.precisionByThreshold
    }

    def printMetrics(aUC: Double, precision: DataFrame, logLossDF: DataFrame, confusionMatrix: DataFrame, accuracy: DataFrame) = {

        println("=============METRICS=============")
        println("Area Under Curve " + aUC)
        println("============PRECISION============")
        precision.show(false)
        println("=============LOGLOSS=============")
        logLossDF.show(false)
        println("========CONFUSION==MATRIX========")
        confusionMatrix.show(false)
        println("=============ACCURACY============")
        accuracy.show(false)
        println("=================================")
    }

}
