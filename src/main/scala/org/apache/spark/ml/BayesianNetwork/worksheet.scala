package org.apache.spark.ml.BayesianNetwork

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{StringIndexer, StandardScaler,QuantileDiscretizer}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{arrays_zip, col, explode, lit, mean, typedLit}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}
// import org.apache.hadoop.fs.http.HttpFileSystem
// import org.apache.spark.mllib.evaluation.MulticlassMetrics
// import org.apache.spark.mllib.regression.LabeledPoint


object worksheet extends Serializable {
  /* Convert StringType Columns using StringIndexer and remove stringtypes */
def process_data(df: DataFrame, threshold: Int = 4): DataFrame = {
    val stringTypeColumns = df.schema.fields.filter(_.dataType == StringType).map(_.name)
    val process_DF = df.schema.fields.foldLeft(df) { (tempDF, field) =>
      if (field.dataType == StringType) {
        val indexer = new StringIndexer()
          .setInputCol(field.name)
          .setOutputCol(s"indexed_${field.name}")
          .fit(tempDF)

        val indexedDF = indexer.transform(tempDF)
        indexedDF
      } else {
        tempDF
      }
    }.drop(stringTypeColumns: _*)

    var newDF = process_DF.columns.foldLeft(process_DF)((tempDf, colName) => {
      val length = process_DF.select(colName).distinct().count()
      if (colName.startsWith("indexed_")){
        tempDf.withColumn(colName, col(colName).cast(IntegerType))
      }
      /* Numerical detector for standardization */
      else if ((process_DF.schema(process_DF.columns.indexOf(colName)).dataType == DoubleType) ||
        ((length > threshold) &&
          (process_DF.schema(process_DF.columns.indexOf(colName)).dataType == IntegerType))) {
        var tmp = tempDf
        val mean = df.selectExpr(s"mean(${colName})").collect()(0)(0).asInstanceOf[Double]
        val stddev = df.selectExpr(s"stddev(${colName})").collect()(0)(0).asInstanceOf[Double]
        val zScoreCol = (col(colName) - mean) / stddev
        tmp = tempDf.withColumn("zscore_" + colName, zScoreCol)

        tmp.drop(colName)
      }
      else tempDf
    })
    val cols = newDF.columns.map(c => {

      if (newDF.schema(newDF.columns.indexOf(c)).dataType == IntegerType) {
        if (c.contains("indexed_")) c.replace("indexed_", "")
        else c
      }
      else c.replace("zscore_", "")
    })
    newDF = newDF.toDF(cols: _*)
    newDF
    
  }

  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = SparkContext.getOrCreate()
    val spark = SparkSession.builder()
    .config("spark.driver.maxResultSize", "0")
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
        /* Iris */
        val customSchema = StructType(Array(
          StructField("feat1", IntegerType, true),
          StructField("feat2", IntegerType, true),
          StructField("feat3", IntegerType, true),
          StructField("feat4", IntegerType, true),
          StructField("class", IntegerType, true)
        ))

        /*Synthetic dataset*/
        // val customSchema = StructType(Array(
        //   StructField("feat0", StringType, true),
        //   StructField("feat1", StringType, true),
        //   StructField("feat2", StringType, true),
        //   StructField("feat3", StringType, true),
        //   StructField("feat4", DoubleType, true),
        //   StructField("feat5", DoubleType, true),
        //   StructField("feat6", DoubleType, true),
        //   StructField("feat7", DoubleType, true),
        //   StructField("feat8", DoubleType, true),
        //   StructField("feat9", DoubleType, true),
        //   StructField("feat10", DoubleType, true),
        //   StructField("feat11", DoubleType, true),
        //   StructField("feat12", DoubleType, true),
        //   StructField("feat13", DoubleType, true),
        //   StructField("feat14", DoubleType, true),
        //   StructField("label", StringType, true)
        // ))


    val path = "file:///opt/bitnami/spark/data/iris.csv"
    // val path = "file:///opt/spark/work-dir/dataset3.csv"

    var df = spark.read.format("csv")
      .option("header", "true")
      // .option("inferSchema", "true")
      .schema(customSchema)
      .load(s"$path")

    df = process_data(df)

  // Iris dataset graph architecture

  val dependencies = Array(
    ("feat1", "class"),
    ("feat2", "class"),
    ("feat3", "class"),
    ("feat4", "class")
  )

  // Synthetic data graph architectures

    /* Setup 1 */

//    val dependencies = Array(
//     ("feat0", "feat10"),
//     ("feat1", "feat10"),
//     ("feat3", "feat10"),
//     ("feat8", "feat10"),
//     //next
//     ("feat0", "feat11"),
//     ("feat1", "feat11"),
//     ("feat3", "feat11"),
//     ("feat4", "feat11"),
//     ("feat5", "feat11"),
//     ("feat6", "feat11"),
//     ("feat9", "feat11"),
//     //next
//     ("feat2", "feat12"),
//     ("feat3", "feat12"),
//     ("feat5", "feat12"),
//     ("feat6", "feat12"),
//     ("feat8", "feat12"),
//     //next
//     ("feat1", "feat13"),
//     ("feat7", "feat13"),
//     ("feat8", "feat13"),
//     //next
//     ("feat0", "feat14"),
//     ("feat2", "feat14"),
//     ("feat4", "feat14"),
//     ("feat5", "feat14"),
//     ("feat6", "feat14"),
//     ("feat7", "feat14"),
//     //next
//     ("feat0", "feat15"),
//     ("feat1", "feat15"),
//     ("feat4", "feat15"),
//     ("feat5", "feat15"),
//     ("feat8", "feat15"),
//     ("feat9", "feat15"),
//     ("feat10", "feat15"),
//     ("feat11", "feat15"),
//     ("feat13", "feat15"),
//     ("feat14", "feat15"),
//     //next
//     ("feat0", "feat16"),
//     ("feat1", "feat16"),
//     ("feat2", "feat16"),
//     ("feat4", "feat16"),
//     ("feat5", "feat16"),
//     ("feat8", "feat16"),
//     ("feat9", "feat16"),
//     ("feat10", "feat16"),
//     ("feat11", "feat16"),
//     ("feat13", "feat16"),
//     ("feat14", "feat16"),
//     //next
//     ("feat4", "feat_17"),
//     ("feat7", "feat_17"),
//     ("feat8", "feat_17"),
//     ("feat10", "feat_17"),
//     ("feat13", "feat_17"),
//     ("feat14", "feat_17"),
//     //next
//     ("feat1", "feat18"),
//     ("feat3", "feat18"),
//     ("feat5", "feat18"),
//     ("feat7", "feat18"),
//     ("feat9", "feat18"),
//     ("feat10", "feat18"),
//     ("feat11", "feat18"),
//     //next
//     ("feat0", "feat19"),
//     ("feat1", "feat19"),
//     ("feat2", "feat19"),
//     ("feat3", "feat19"),
//     ("feat6", "feat19"),
//     ("feat7", "feat19"),
//     ("feat8", "feat19"),
//     ("feat10", "feat19"),
//     ("feat14", "feat19"),
//     //next
//     ("feat0", "label"),
//     ("feat1", "label"),
//     ("feat2", "label"),
//     ("feat3", "label"),
//     ("feat4", "label"),
//     ("feat5", "label"),
//     ("feat6", "label"),
//     ("feat7", "label"),
//     ("feat8", "label"),
//     ("feat9", "label"),
//     ("feat10", "label"),
//     ("feat11", "label"),
//     ("feat12", "label"),
//     ("feat13", "label"),
//     ("feat14", "label"),
//     ("feat15", "label"),
//     ("feat16", "label"),
//     ("feat17", "label"),
//     ("feat18", "label"),
//     ("feat19", "label")
//    )

    /* Setup 2 */

//    val dependencies = Array(
//     ("feat0", "label"),
//     ("feat1", "label"),
//     ("feat2", "label"),
//     ("feat3", "label"),
//     ("feat4", "label"),
//     ("feat5", "label"),
//     ("feat6", "label"),
//     ("feat7", "label"),
//     ("feat8", "label"),
//     ("feat9", "label"),
//     ("feat10", "label"),
//      ("feat11", "label"),
//      ("feat12", "label"),
//      ("feat13", "label"),
//      ("feat14", "label"),
//      ("feat15", "label"),
//      ("feat16", "label"),
//      ("feat17", "label"),
//      ("feat18", "label"),
//      ("feat19", "label")
//    )

    /* Setup 3 */

//   val dependencies = Array(
//     ("feat3", "feat_14"),
// ("feat3", "feat15"),
// ("feat3", "feat16"),
// ("feat4", "feat14"),
// ("feat4", "feat15"),
// ("feat4", "feat16"),
// ("feat6", "feat12"),
// ("feat6", "feat15"),
// ("feat8", "feat14"),
// ("feat8", "feat15"),
// ("feat8", "feat16"),
// ("feat9", "feat10"),
// ("feat9", "feat12"),
// ("feat9", "feat14"),
// ("feat9", "feat15"),
// ("feat9", "feat16"),
// ("feat10", "feat12"),
// ("feat10", "feat16"),
// ("feat11", "feat13"),
// ("feat11", "feat15"),
// ("feat11", "feat16"),
// ("feat12", "feat14"),
// ("feat12", "feat15"),
// ("feat13", "feat15"),
// ("feat13", "feat16"),
// ("feat13", "feat18"),
// ("feat14", "feat15"),
// ("feat14", "feat16"),
// ("feat15", "feat16"),
//      ("feat11", "label"),
//      ("feat12", "label"),
//      ("feat13", "label"),
//      ("feat14", "label"),
//      ("feat15", "label"),
//      ("feat16", "label"),
//      ("feat17", "label"),
//      ("feat18", "label"),
//      ("feat19", "label")
//   )

    /*  */

    /* Initialization of the SparkDBN model */

    val Array(trainingData, testData) = df.randomSplit(Array(0.8, 0.2), seed=2222L)

    val train = trainingData.cache()
    val test = testData.cache()

    val sparkDBN = new BayesianNetwork()

    val start_training = System.nanoTime()
    println("Runtime of SparkDBN")
    println("Training phase of SparkDBN")
    sparkDBN.fit(train, dependencies)
    val stop_training = System.nanoTime()

    import spark.implicits._
    import org.apache.spark.mllib.evaluation.MulticlassMetrics
    import org.apache.spark.rdd.RDD

    println("Evaluation of SparkDBN")

    val start_testing = System.nanoTime()
    val predictionRDD = sparkDBN.predict_with_labels(test,"label")
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
      // println(metrics_1.precision(l))
    }
    // Recall by label
    labels.foreach { l =>
      println(s"Recall($l) = " + metrics.recall(l))
      // println(metrics_1.recall(l))
    }
    // F-measure by label
    labels.foreach { l =>
      println(s"F1-Score($l) = " + metrics.fMeasure(l))
      // println(metrics_1.fMeasure(l))
    }
      println(s"Execution time for training: $duration_training seconds")
      println(s"Execution time for testing: $duration_testing seconds")
      
  }
}