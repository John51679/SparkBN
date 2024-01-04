package org.apache.spark.ml.BayesianNetwork

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{StringIndexer, StandardScaler, QuantileDiscretizer}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{arrays_zip, col, explode, lit, mean, typedLit, stddev}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}


object worksheet extends Serializable {
    /* Standarize the data */
    def process_data(df: DataFrame, threshold: Int = 25): DataFrame = {
        var newDF = df.columns.foldLeft(df)((tempDf, colName) => {
            if(df.schema(df.columns.indexOf(colName)).dataType == DoubleType) {
                val avg = df.select(mean(colName)).collect()(0)(0).asInstanceOf[Double]
                val sd = df.select(stddev(colName)).collect()(0)(0).asInstanceOf[Double]
                val tmp = tempDf.withColumn("zscore_" + colName, (col(colName) - avg) / sd)
                tmp.drop(colName)
            }
            else {
                tempDf
            }
        })
        val cols = newDF.columns.map(c => c.replace("zscore_", ""))
        newDF = newDF.toDF(cols: _*)
        newDF
    }

    def main(args: Array[String]): Unit = {

        if(args.length == 0) {
            println("No dataset path provided. Exiting...")
            sys.exit(0)
        }
        val path = args(0)

        Logger.getLogger("org").setLevel(Level.ERROR)
        val sc = SparkContext.getOrCreate()
        val spark = SparkSession.builder()
            .config("spark.driver.maxResultSize", "0")
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        spark.sparkContext.setCheckpointDir("/datasets")

        var df = spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(s"$path")

        val dependencies = new Array[(String, String)](df.columns.length - 1)
        var i = 0
        for(col <- df.columns) {
            if(col != "label") {
                dependencies(i) = (col, "label")
                i = i + 1
            }
        }

        df = process_data(df)

        /* Initialization of the SparkBN model */

        val Array(trainingData, testData) = df.randomSplit(Array(0.8, 0.2), seed = 2222L)

        trainingData.cache()
        trainingData.checkpoint()
        testData.cache()
        testData.checkpoint()

        val sparkBN = new BayesianNetwork()

        val start_training = System.nanoTime()
        println("Runtime of SparkBN")
        println("Training phase of SparkBN")
        sparkBN.fit(trainingData, dependencies)
        val stop_training = System.nanoTime()

        import spark.implicits._
        import org.apache.spark.mllib.evaluation.MulticlassMetrics
        import org.apache.spark.rdd.RDD

        println("Evaluation of SparkBN")

        val start_testing = System.nanoTime()
        val predictionRDD = sparkBN.predict_with_labels(testData, "label", 0.00001)
        val stop_testing = System.nanoTime()

        val duration_training = (stop_training - start_training) / 1e9d
        val duration_testing = (stop_testing - start_testing) / 1e9d

        val metrics = new MulticlassMetrics(predictionRDD)
        val accuracy = metrics.accuracy

        println("Summary Statistics")
        println(s"Accuracy = $accuracy")
        // Precision by label
        val labels = metrics.labels
        labels.foreach { l =>
            println(s"Precision($l) = " + metrics.precision(l))
        }
        // Recall by label
        labels.foreach { l =>
            println(s"Recall($l) = " + metrics.recall(l))
        }
        // F-measure by label
        labels.foreach { l =>
            println(s"F1-Score($l) = " + metrics.fMeasure(l))
        }
        println(s"Execution time for training: $duration_training seconds")
        println(s"Execution time for testing: $duration_testing seconds")

    }
}