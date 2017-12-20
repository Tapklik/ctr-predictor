package model

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import org.apache.spark.sql.DataFrame
import core.Paths
import scala.io.Source


class Model (modelSize: Int, loadable: Boolean, saveable: Boolean, trainingData: DataFrame, testingData: DataFrame,
             columns: Array[String]) extends Serializable {

    var n : Array[Double] = Array.fill[Double](modelSize)(0.0)
    var z : Array[Double] = Array.fill[Double](modelSize)(0.0)
    var w : Array[Double] = Array.fill[Double](modelSize)(0.0)
    private var noOfRuns = 8
    private var count = testingData.count()

    if(loadable){
        n = Model.loadModel(modelSize, Paths.npath)
        z = Model.loadModel(modelSize, Paths.zpath)
    }

    for(i <- Range(0, noOfRuns)){
        Model.trainModel(trainingData, columns, n, z, w)
    }

    if(saveable){
        Model.saveModel(Paths.npath, n)
        Model.saveModel(Paths.zpath, z)
    }

    var (logLossBase, tp, tn, fp, fn) = Model.testModel(testingData, columns, n, z, w)

    Model.getMetrics(tp, tn, fp, fn, logLossBase, count)
}

class LineModel (modelSize: Int, predictData: DataFrame) extends Serializable {

    var n : Array[Double] = Model.loadModel(modelSize, Paths.npath)
    var z : Array[Double] = Model.loadModel(modelSize, Paths.zpath)
    var w : Array[Double] = Array.fill[Double](modelSize)(0.0)

    predictData.collect().foreach{ row =>
        var values = List[Int]()
        List.range(0, row.length - 1).foreach { j =>
            values ::= row(j).toString.toInt
        }
        var prediction = Model.predict(values, n, z, w)
        println(prediction)
    }

}

object Model {

    def predict(values : List[Int], n : Array[Double], z : Array[Double], w : Array[Double]) : Double ={
        var sign = 1
        var wTx = 0.0
        var p = 0.0

        values.foreach{ i =>
            if(z(i) < 0.0){
                sign = -1
            }
            if(sign*z(i) <= 0.1 /*L1*/){
                w(i) = 0
            }
            else{
                w(i) = (sign * 1.0 - z(i)) / ((1.0 /*beta*/ + math.sqrt(n(i))) / 1.0 + 1.0 /*alpha + L2*/ )
            }
            wTx += w(i)
        }

        p = 1.0 / (1.0 + math.exp(-math.max(math.min(wTx, 35.0), -35.0)))

        p

    }

    def update(values : List[Int], p : Double, click : Int,
               n : Array[Double], z : Array[Double],
               w : Array[Double]) = {
        var grad = 0.0
        var sigma = 0.0
        grad = p - click

        values.foreach{ i =>
            sigma = (math.sqrt(n(i) + grad*grad) - math.sqrt(n(i))) / 1.0 /*alpha*/
            z(i) += grad-sigma*w(i)
            n(i) += grad*grad
        }
    }

    def saveModel(path: String, data: Array[Double]) = {
        val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path)))
        for (x <- data) {
            writer.write(x + "\n")
        }
        writer.close()
    }

    def loadModel(mapSize : Int, path : String) : Array[Double] = {
        var model = Array.fill[Double](mapSize)(0.0)
        var loadCount = 0

        for (line <- Source.fromFile(path).getLines) {
            model(loadCount) += line.toDouble
            loadCount += 1
        }

        model
    }

    def getMetrics(tp: Int, tn: Int, fp: Int, fn: Int, logLossBase: Double, count: Double) = {

        var totalLogLoss = -logLossBase/count.toDouble

        println("Log Loss: " + totalLogLoss)

        println("TRUE: ")
        println("Negatives: " + tn)
        println("Positives: " + tp)
        println("FALSE: ")
        println("Negatives: " + fn)
        println("Positives: " + fp)
        println("Accuracy: " + (tp+tn)/count.toDouble)
    }

    def trainModel(trainingData: DataFrame, columns: Array[String], n: Array[Double], z: Array[Double], w: Array[Double]) = {

        trainingData.collect().foreach{ row =>
            var click = row(0).toString.toInt
            var values = List[Int]()
            List.range(1, columns.length - 2).foreach { j =>
                values ::= row(j).toString.toInt
            }
            var prediction = predict(values, n, z, w)
            update(values, prediction, click, n, z, w)
        }

    }

    def testModel(testingData: DataFrame, columns: Array[String], n: Array[Double], z: Array[Double], w: Array[Double]) = {

        var logLossBase = 0.0
        var tn, tp, fn, fp = 0

        testingData.collect().foreach{ row =>
            var click = row(0).toString.toDouble
            var values = List[Int]()
            List.range(1, columns.length - 2).foreach { j =>
                values ::= row(j).toString.toInt
            }
            var prediction = predict(values, n, z, w)

            logLossBase += calculateLogLoss(click, prediction)

            if(prediction < 0.5){
                if(click == 0.0){
                    tn += 1
                }
                else{
                    fn += 1
                }
            }
            else{
                if(click == 1.0){
                    tp += 1
                }
                else{
                    fp += 1
                }
            }
        }

        (logLossBase, tp, tn, fp, fn)

    }

    def calculateLogLoss(label: Double, prediction_1: Double) = {
        var prediction_0 = 1 - prediction_1

        var sum = 0.0

        if (label == 0.0){
            var y_0 = 1
            var y_1 = 0
            var r_0 = 0.0
            var r_1 = 0.0
            r_0 = y_0 * math.log(math.max(math.min(prediction_0, 1 - 10E-15), 10E-15))
            r_1 = y_1 * math.log(math.max(math.min(prediction_1, 1 - 10E-15), 10E-15))
            sum = r_0 + r_1
        }
        else{
            var y_0 = 0
            var y_1 = 1
            var r_0 = 0.0
            var r_1 = 0.0
            r_0 = y_0 * math.log(math.max(math.min(prediction_0, 1 - 10E-15), 10E-15))
            r_1 = y_1 * math.log(math.max(math.min(prediction_1, 1 - 10E-15), 10E-15))
            sum = r_0 + r_1
        }

        sum
    }

}

