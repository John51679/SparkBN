package org.apache.spark.ml.BayesianNetwork

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{coalesce, col, concat, lit, mean, stddev, sum}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StructField}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import math.{Pi, exp, pow, sqrt}

  class BayesianNetwork() {
    /* graph that will be used throughout this class */
    private var graph: Graph[Array[Tuple3[String, Double, Array[Tuple2[Double, Double]]]], Array[Double]] = null
    private val sc = SparkContext.getOrCreate()

    /* Creates the graph from the context given by the user (i.e dependency array and the DataFrame)
  *  Many ideas to be added. For example a method could be added for a naive bayes approach since
  *  it has a straightforward graph representation.
  *
  * */
    def __createGraphFromData__(dependencies: Array[(String, (String))], data: DataFrame): Unit = {
      //Graph[Array[Tuple3[String,Double,Array[Double]]],(Long,Long)] = {

      /* Tuple2-> each node will be represented by a prefix to determine its query code
    * (something like a query having its own ID for indexing) and a Double value which
    * will contain the probability for that query ID
    *  */
      val arr: Array[(VertexId, Array[Tuple3[String, Double, Array[Tuple2[Double, Double]]]])] =
        new Array[(VertexId, Array[Tuple3[String, Double, Array[Tuple2[Double, Double]]]])](data.columns.length)
      for (i <- data.columns.indices) {
        arr(i) = (i.asInstanceOf[Long], Array(("empty", -1.0, Array((0.0, 0.0)))))
      }
      val nodes: RDD[(VertexId, Array[Tuple3[String, Double, Array[Tuple2[Double, Double]]]])] = sc.parallelize(arr)
      /* */

      /* Create the edges that were given in dependencies array */

      val edges_arr: Seq[Edge[Array[Double]]] = dependencies.map(elem => {
        Edge[Array[Double]](data.columns.indexOf(elem._1).asInstanceOf[Long], data.columns.indexOf(elem._2).asInstanceOf[Long], Array(-99999.0))
      }).toSeq
      val edges: RDD[Edge[Array[Double]]] = sc.parallelize(edges_arr)

      /* graph construction */
      this.graph = Graph(nodes, edges)

    }

    def __calculateCPT__(vertexId: VertexId, data: DataFrame): Unit = {
      /* for the vertex given as input to this function, find all incoming neighbors and produce
        * a new dataframe keeping only the columns corresponding to the currently processed vertex
        * and its neighbors (p_data variable). Then for the produced p_data dataframe a new vertexid
        * is calculated to match the p_data vertex id (p_vertexid variable)
        *
        * sorted_vertex_arr ensures that data will never be shuffled and calculated in a specific way
        * to avoid information loss or scramble.
        * */
      val edge = this.graph.edges.filter(_.dstId == vertexId).collect()

      val vertexArr = edge.map { f => f.srcId }.filter(p => p != -1L)

      val cols = (vertexArr :+ vertexId).sorted.map(idx => data.columns(idx.toInt))

      val p_data = data.select(cols.head, cols.tail: _*)
      val discrete_data = p_data.select(
        p_data.columns.filter(colName => {
          p_data.schema(colName).dataType == IntegerType
        }).map(col): _*
      )

      val discreteCols = discrete_data.columns

      val tester = discreteCols.filter(p => p != data.columns(vertexId.toInt))
      val groupedDiscreteDF = discrete_data.groupBy(discreteCols.head, discreteCols.tail: _*).count()

      val resDF = if (tester.nonEmpty) {

        val groupedDF = groupedDiscreteDF.groupBy(discreteCols.head, discreteCols.tail: _*).agg(sum("count").alias("count"))
        val totalCountDF = groupedDiscreteDF.groupBy(tester.head, tester.tail: _*).agg(sum("count").alias("total_count"))
        val joinedDF = groupedDF.join(totalCountDF, Seq(tester: _*))
        joinedDF
          .withColumn("probability", col("count") / col("total_count"))
          .withColumn("prefix", concat(discreteCols.map(col): _*))

      } else {
        val denominator = discrete_data.count().toDouble
        groupedDiscreteDF
          .withColumn("probability", col("count") / denominator)
          .withColumn("prefix", concat(discreteCols.map(col): _*))
      }

      val stdMeanData = p_data.select(
        p_data.columns.filter(colName => {
          (p_data.schema(colName).dataType != IntegerType) || (colName == data.columns(vertexId.toInt))
        }).map(col): _*)

      val cont_cols = stdMeanData.columns.filter(p => p != data.columns(vertexId.toInt))
      val summaryData: DataFrame = if (cont_cols.nonEmpty) {

        stdMeanData
          .groupBy(data.columns(vertexId.toInt))
          .agg(
            cont_cols.flatMap(c =>
              Seq(
                mean(c).as(s"mean_$c"),
                stddev(c).as(s"stddev_$c")
              )).head,
            cont_cols.flatMap(c =>
              Seq(
                mean(c).as(s"mean_$c"),
                stddev(c).as(s"stddev_$c")
              )).tail: _*
          )
      } else stdMeanData

      val intermediateDF = resDF.select("prefix", "probability", data.columns(vertexId.toInt))
      val res: Array[Tuple3[String, Double, Array[Tuple2[Double, Double]]]] =
        if (cont_cols.nonEmpty) {
          val resultDF = intermediateDF.join(summaryData, data.columns(vertexId.toInt))
          resultDF.rdd.map(row => {
            val prefix = row.getAs[String]("prefix")
            val probability = row.getAs[Double]("probability")
            val meansAndStddevs =
              cont_cols.map(c => {
                val meanVal = row.getAs[Double](s"mean_$c")
                val stddevVal = row.getAs[Double](s"stddev_$c")
                (meanVal, stddevVal)
              })
            (prefix, probability, meansAndStddevs)
          }).collect()
        } else {
          intermediateDF.rdd.map(row => {
            val prefix = row.getAs[String]("prefix")
            val probability = row.getAs[Double]("probability")
            (prefix, probability, Array.empty[(Double, Double)])
          }).collect()
        }

      this.graph = this.graph.mapVertices((vid, tup) => if (vid == vertexId) res else tup)
    }

    def __combinationIterator__(lengths: Array[Int]): Iterator[Array[Int]] = {
      val combinations = lengths.product
      val combination_arr = Array.fill(combinations)(Array.fill(lengths.length)(0))

      for (i <- 0 until combinations) {
        var temp = i
        for (j <- lengths.indices.reverse) {
          combination_arr(i)(j) = temp % lengths(j)
          temp /= lengths(j)
        }
      }
      combination_arr.iterator
    }

    def __calculateMeanStd__(vertexId: VertexId, data: DataFrame): Unit = {
      val edge = this.graph.edges.filter(ed => ed.dstId == vertexId).filter(p => p.srcId != -1L && p.dstId != 1L)
      if (edge.isEmpty()) {
        val tmp = data.select(mean(data.columns(vertexId.toInt)), coalesce(stddev(data.columns(vertexId.toInt)), lit(1.0))).first()
        val meanStd = (tmp.getDouble(0), tmp.getDouble(1))
        val res: Array[Tuple3[String, Double, Array[Tuple2[Double, Double]]]] = Array(("no_prefix", 0.0, Array(meanStd)))

        this.graph = this.graph.mapVertices((vid, tup) => if (vid == vertexId) res else tup)
      } else {
        val vertex_arr: Array[VertexId] = edge.map(ed => ed.srcId).collect()
        val sorted_vertex_arr = vertex_arr.sorted
        val cols = (sorted_vertex_arr :+ vertexId).map(idx => data.columns(idx.toInt))
        val p_data = data.select(cols.head, cols.tail: _*)
        val discrete_data = p_data.select(
          p_data.columns.filter(colName => {
            p_data.schema(colName).dataType == IntegerType
          }).map(col): _*
        )
        if (!discrete_data.columns.isEmpty) {
          val lengths: Array[Int] = discrete_data.columns.map(str => {
            val count = discrete_data.select(str).distinct().count()
            count.asInstanceOf[Int]
          })
          val it = __combinationIterator__(lengths).toArray /* Point of interest for optimization */
          val columnName = data.columns(vertexId.toInt)
          val tmp = data
            .groupBy(discrete_data.columns.head, discrete_data.columns.tail:_*)
            .agg(
                  coalesce(mean(columnName),lit(0.0)).as(s"mean_$columnName"),
                  coalesce(stddev(columnName),lit(0.0)).as(s"stddev_$columnName")
            )

          val resDF = tmp.withColumn("prefix", concat(discrete_data.columns.map(col): _*))
          val vertex_res: Array[Tuple3[String, Double, Array[Tuple2[Double, Double]]]] = {
              resDF.rdd.map(row => {
                val prefix = row.getAs[String]("prefix")
                val mean = row.getAs[Double](s"mean_${columnName}")
                val stddev = row.getAs[Double](s"stddev_${columnName}")
                (prefix,0.0,Array((mean,stddev)))
              }).collect()
          }
          this.graph = this.graph.mapVertices((vid, tup) => if (vid == vertexId) vertex_res else tup)

          val fields = data.schema.fields
          val cont_edges = edge.filter(ed => fields(ed.srcId.toInt).dataType != IntegerType).collect()
          if (!cont_edges.isEmpty) {
            cont_edges.foreach(ed => {
              val edges_res: Array[Double] = it.indices.map(index => {
                val tmp = p_data.select(mean(data.columns(ed.srcId.toInt)), coalesce(stddev(data.columns(ed.srcId.toInt)), lit(1.0))).first()
                val meanStd_src = (tmp.getDouble(0), tmp.getDouble(1))

                val mean_dst: Double = vertex_res.map(tup => {
                  if (tup._1 == it(index).mkString) tup._3(0)._1
                  else -1.0
                }).max
                val std_dst: Double = vertex_res.map(tup => {
                  if (tup._1 == it(index).mkString) tup._3(0)._2
                  else -1.0
                }).max
                val test =
                  (col(data.columns(ed.srcId.toInt)) - meanStd_src._1) * (col(data.columns(ed.dstId.toInt)) - mean_dst)
                val df = p_data.withColumn("Covariance", test).selectExpr("mean(Covariance)").first().getDouble(0)
                val correlation = df / (meanStd_src._2 * std_dst)

                correlation

              }).toArray
              this.graph = this.graph.mapEdges(g_ed => if (g_ed.srcId == ed.srcId) edges_res else g_ed.attr)
            })
          }
          /* Case where Vid has no discrete neighbors*/
        } else {
          val tmp_dst = p_data.select(mean(data.columns(vertexId.toInt)), coalesce(stddev(data.columns(vertexId.toInt)), lit(1.0))).first()
          val meanStd_dst = (tmp_dst.getDouble(0), tmp_dst.getDouble(1))

          val vertex_res = Array(("no_profix", 0.0, Array((meanStd_dst._1, meanStd_dst._2))))
          this.graph = this.graph.mapVertices((vid, tup) => if (vid == vertexId) vertex_res else tup)

          edge.collect().foreach(ed => {
            val tmp_src = p_data.select(mean(data.columns(ed.srcId.toInt)), coalesce(stddev(data.columns(ed.srcId.toInt)), lit(1.0))).first()
            val meanStd_src = (tmp_src.getDouble(0), tmp_src.getDouble(1))

            val test =
              (col(data.columns(ed.srcId.toInt)) - meanStd_src._1) * (col(data.columns(ed.dstId.toInt)) - meanStd_dst._1)

            val df = p_data.withColumn("Covariance", test).selectExpr("mean(Covariance)").first().getDouble(0)
            val correlation = df / (meanStd_src._2 * meanStd_dst._2)

            this.graph = this.graph.mapEdges(g_ed => if (g_ed.srcId == ed.srcId) Array(correlation) else g_ed.attr)

          })
        }
      }
    }

    def fit(data: DataFrame, dependencies: Array[(String, String)]): Unit = {
      __createGraphFromData__(dependencies, data)
      for (vertexId <- data.columns.indices) {
        if (data.schema.fields(vertexId).dataType == IntegerType) {
          __calculateCPT__(vertexId, data)
        } else {
          __calculateMeanStd__(vertexId, data)
        }
      }
    }

    def __maximum_likelihood__(test_data: DataFrame, vertexId: VertexId, default_prob: Double): RDD[Int] = {
      /* for the vertex given as input to this function, find all incoming neighbors and produce
     * a new dataframe keeping only the columns corresponding to the currently processed vertex
     * and its neighbors (p_data variable)
     * */

      val length = test_data.select(col(test_data.columns(vertexId.toInt))).distinct().count().toInt

      /* This map operation processes each row and from it determines the prefix (ID) of the query
    *  that is asked. Then it simply indexes that ID within the graph in the appropriate vertexID
    *  and returns the probability that is stored within. This works even if there is NaN probability
    *  (i.e the query combination was not found at all within the dataset) as it will simply output
    *  -1.0 as the probability and pick category 0 for an answer on this. If that happens then it means
    *  that its a dataset issue and not something that the model should take care of
    *
    *  returns an array of rows where each row is the predicted value (i.e the maximum likelihood of an
    *  event based on observations.
    * */

      val vertices = this.graph.vertices.filter(p => p._1 != -1L).sortBy(_._1).collect()

      val edges = this.graph.edges.filter(p => p.srcId != -1L && p.dstId != -1L).collect()
      //val edges = edge_arr.filter(f => (f.dstId != -1L) || (f.srcId != -1L))
      val fields_arr: Array[StructField] = test_data.schema.fields

      test_data.rdd.map(row => {
        val combination_row = row.toSeq.toArray
        /* prob will have a length of the distinct output class values and each index is the class category */
        val prob = new Array[Double](length)

        for (category <- 0 until length) {
          combination_row(vertexId.toInt) = category
          prob(category) =
          /* This vertices map operation implements the Joint probability calculation following the formula
        *  P(X1,X2,...,Xn) = Pi(i->n) (P(Xi | Parents(Xi))
        *  */
            vertices.map(f => {
              /* for each Xi find its parents */
              val neighbors = edges.filter(p => p.dstId == f._1).map(p => p.srcId)
              /* if it has no parents then go and calculate P(Xi) directly from CPT if its discrete,
            * calculate its pdf if its continuous */
              if (!neighbors.nonEmpty) {
                if (fields_arr(f._1.toInt).dataType == IntegerType) {
                  val filtered = f._2.filter(p => p._1 == combination_row(f._1.toInt).toString)
                  if (filtered.nonEmpty) filtered(0)._2
                  else 0.0
                } else {
                  val std = f._2(0)._3(0)._2
                  val mean = f._2(0)._3(0)._1
                  1.0 / (sqrt(2.0 * Pi) * std) * exp(-(1.0 / 2.0) * pow((combination_row(f._1.toInt).asInstanceOf[Double] - mean) / std, 2))
                }
              }
              /* this else part refers to the case where Xi has parents.
            *  neighbors will have its parents stored and later on iterated
            *  */
              else {
                /* checks whether Xi is categorical or not. If it is then sort all neighbors and add the Xi to the array
              *  Then take its query ID (combination variable) and fetch its data within the graph through fetchedData.
              *  Then check if there are continuous data in dependence with Xi and split into prob_discrete and
              *  prob_continuous. If continuous data exist simply do prob_continuous * prob_discrete (might change)
              *  else simply return prob_discrete from CPT.
              *  */
                if (fields_arr(f._1.toInt).dataType == IntegerType) {
                  val sorted_vertex_arr = (neighbors :+ f._1).sorted
                  val indices = sorted_vertex_arr.filter(p => fields_arr(p.toInt).dataType == IntegerType).sorted
                  val combination: String = indices.map(value => combination_row(value.toInt)).mkString

                  val filtered = f._2.filter(p => p._1 == combination)

                  val fetchedData = if (filtered.nonEmpty) {
                    filtered(0)
                  } else ("no_prefix", default_prob, Array.empty[(Double, Double)])

                  val prob_discrete = fetchedData._2
                  var prob_continuous = 0.0
                  //Checks whether discrete vertex Xi has continuous neighbors
                  if (fetchedData._3.nonEmpty) {
                    val doubletype_indexes = sorted_vertex_arr.filter(p => fields_arr(p.toInt).dataType != IntegerType).map(_.toInt).sorted

                    prob_continuous = fetchedData._3.map(mean_std => {
                      val index = fetchedData._3.indexOf(mean_std)
                      if (mean_std._2 != 0.0) {
                        1.0 / (sqrt(2.0 * Pi) * mean_std._2) * exp(-(1.0 / 2.0) * pow((combination_row(doubletype_indexes(index)).asInstanceOf[Double] - mean_std._1) / mean_std._2, 2))
                      } else {
                        0.0
                      }
                    }).product
                    prob_continuous * prob_discrete
                  } else prob_discrete
                } else {

                  /* To be added. this is the case where Xi itself is continuous data and has parents */
                  //                val discrete_neighbors = neighbors.foldLeft(Array(-1L))((res, vid) => {
                  //                  if (fields_arr(vid.toInt).dataType == IntegerType) res :+ vid
                  //                  else res
                  //                }).filter(value => value != -1L)
                  val discrete_neighbors = neighbors.filter(p => fields_arr(p.toInt).dataType == IntegerType).sorted
                  /* This is the case where Xi has discrete parents */
                  if (!discrete_neighbors.isEmpty) {

                      val index = discrete_neighbors.sorted.map(vid => row(vid.toInt).asInstanceOf[Int]).mkString
                      val ed_index = f._2.indexWhere(p => p._1 == index)
                      val fetchedData = if (ed_index != -1)
                      {
                        f._2(ed_index)
                      } else {
                        ("no_prefix", default_prob, Array((0.0,1.0)))
                      }

                      val mean_dst = fetchedData._3(0)._1
                      val std_dst = fetchedData._3(0)._2
                      val cont_edges = edges.filter(ed => ed.dstId == f._1 && fields_arr(ed.srcId.toInt).dataType != IntegerType)
                    val cont_prob = cont_edges.map(ed => {
                        val mean_src = vertices(ed.srcId.toInt)._2(0)._3(0)._1
                        val std_src = vertices(ed.srcId.toInt)._2(0)._3(0)._2
                        val correlation = ed.attr(ed_index)
                        val S = pow((row(ed.srcId.toInt).asInstanceOf[Double] - mean_src) / std_src, 2) +
                          pow((row(ed.dstId.toInt).asInstanceOf[Double] - mean_dst) / std_dst, 2) -
                          2.0 * correlation * (row(ed.srcId.toInt).asInstanceOf[Double] - mean_src) * (row(ed.dstId.toInt).asInstanceOf[Double] - mean_dst) / (std_src * std_dst)
                        val joint_prob = 1.0 / (2.0 * Pi * std_src * std_dst * sqrt(1.0 - pow(correlation, 2))) * exp(-S / (2.0 * (1.0 - pow(correlation, 2))))
                        joint_prob
                      }).product
                      cont_prob
                  }
                  /* This is the case where Xi has only continuous parents */
                  else {
                    val mean_dst = f._2(0)._3(0)._1
                    val std_dst = f._2(0)._3(0)._2
                    val cont_edges = edges.filter(ed => ed.dstId == f._1 && fields_arr(ed.srcId.toInt).dataType != IntegerType)
                    val cont_prob = cont_edges.map(ed => {
                      val mean_src = vertices(ed.srcId.toInt)._2(0)._3(0)._1
                      val std_src = vertices(ed.srcId.toInt)._2(0)._3(0)._2
                      val correlation = ed.attr(0)
                      val S = pow((row(ed.srcId.toInt).asInstanceOf[Double] - mean_src) / std_src, 2) +
                        pow((row(ed.dstId.toInt).asInstanceOf[Double] - mean_dst) / std_dst, 2) -
                        2.0 * correlation * (row(ed.srcId.toInt).asInstanceOf[Double] - mean_src) * (row(ed.dstId.toInt).asInstanceOf[Double] - mean_dst) / (std_src * std_dst)
                      val joint_prob = 1.0 / (2.0 * Pi * std_src * std_dst * sqrt(1.0 - pow(correlation, 2))) * exp(-S / (2.0 * (1.0 - pow(correlation, 2))))
                      joint_prob
                    }).product

                    cont_prob
                  }
                }
              }
            }
              /* return the product of all Xi|Parents(Xi) */
            ).product.abs
        }
        prob.indexOf(prob.max)
      })
    }

    def predict(test_data: DataFrame, output_node: String, default_prob: Double = 0.0): RDD[Int] = {
      val vid = test_data.columns.indexOf(output_node).toLong
      __maximum_likelihood__(test_data, vid, default_prob)
    }

    /* Classic score functionality. Predicts each row of the testing dataset and prints out
  *  its prediction accuracy. Uses __maximum_likelihood__ as the source of prediction.
  *  */
    def predict_with_labels(test_data: DataFrame, output_node: String, default_prob: Double = 0.0): RDD[(Double,Double)] = {
        val vid = test_data.columns.indexOf(output_node)
        val vertexId = vid.toLong
        
         /* for the vertex given as input to this function, find all incoming neighbors and produce
     * a new dataframe keeping only the columns corresponding to the currently processed vertex
     * and its neighbors (p_data variable)
     * */

      val length = test_data.select(col(test_data.columns(vertexId.toInt))).distinct().count().toInt
      
      /* This map operation processes each row and from it determines the prefix (ID) of the query
    *  that is asked. Then it simply indexes that ID within the graph in the appropriate vertexID
    *  and returns the probability that is stored within. This works even if there is NaN probability
    *  (i.e the query combination was not found at all within the dataset) as it will simply output
    *  -1.0 as the probability and pick category 0 for an answer on this. If that happens then it means
    *  that its a dataset issue and not something that the model should take care of
    *
    *  returns an array of rows where each row is the predicted value (i.e the maximum likelihood of an
    *  event based on observations.
    * */

      val vertices = this.graph.vertices.filter(p => p._1 != -1L).sortBy(_._1).collect()

      val edges = this.graph.edges.filter(p => p.srcId != -1L && p.dstId != -1L).collect()
      //val edges = edge_arr.filter(f => (f.dstId != -1L) || (f.srcId != -1L))
      val fields_arr: Array[StructField] = test_data.schema.fields

      test_data.rdd.map(row => {
        val combination_row = row.toSeq.toArray
        val true_row = row.toSeq.toArray
        /* prob will have a length of the distinct output class values and each index is the class category */
        val prob = new Array[Double](length)

        for (category <- 0 until length) {
          combination_row(vertexId.toInt) = category
          prob(category) =
          /* This vertices map operation implements the Joint probability calculation following the formula
        *  P(X1,X2,...,Xn) = Pi(i->n) (P(Xi | Parents(Xi))
        *  */
            vertices.map(f => {
              /* for each Xi find its parents */
              val neighbors = edges.filter(p => p.dstId == f._1).map(p => p.srcId)
              /* if it has no parents then go and calculate P(Xi) directly from CPT if its discrete,
            * calculate its pdf if its continuous */
              if (!neighbors.nonEmpty) {
                if (fields_arr(f._1.toInt).dataType == IntegerType) {
                  val filtered = f._2.filter(p => p._1 == combination_row(f._1.toInt).toString)
                  if (filtered.nonEmpty) filtered(0)._2
                  else 0.0
                } else {
                  val std = f._2(0)._3(0)._2
                  val mean = f._2(0)._3(0)._1
                  1.0 / (sqrt(2.0 * Pi) * std) * exp(-(1.0 / 2.0) * pow((combination_row(f._1.toInt).asInstanceOf[Double] - mean) / std, 2))
                }
              }
              /* this else part refers to the case where Xi has parents.
            *  neighbors will have its parents stored and later on iterated
            *  */
              else {
                /* checks whether Xi is categorical or not. If it is then sort all neighbors and add the Xi to the array
              *  Then take its query ID (combination variable) and fetch its data within the graph through fetchedData.
              *  Then check if there are continuous data in dependence with Xi and split into prob_discrete and
              *  prob_continuous. If continuous data exist simply do prob_continuous * prob_discrete (might change)
              *  else simply return prob_discrete from CPT.
              *  */
                if (fields_arr(f._1.toInt).dataType == IntegerType) {
                  val sorted_vertex_arr = (neighbors :+ f._1).sorted
                  val indices = sorted_vertex_arr.filter(p => fields_arr(p.toInt).dataType == IntegerType).sorted
                  val combination: String = indices.map(value => combination_row(value.toInt)).mkString

                  val filtered = f._2.filter(p => p._1 == combination)

                  val fetchedData = if (filtered.nonEmpty) {
                    filtered(0)
                  } else ("no_prefix", default_prob, Array.empty[(Double, Double)])

                  val prob_discrete = fetchedData._2
                  var prob_continuous = 0.0
                  //Checks whether discrete vertex Xi has continuous neighbors
                  if (fetchedData._3.nonEmpty) {
                    val doubletype_indexes = sorted_vertex_arr.filter(p => fields_arr(p.toInt).dataType != IntegerType).map(_.toInt).sorted

                    prob_continuous = fetchedData._3.map(mean_std => {
                      val index = fetchedData._3.indexOf(mean_std)
                      if (mean_std._2 != 0.0) {
                        1.0 / (sqrt(2.0 * Pi) * mean_std._2) * exp(-(1.0 / 2.0) * pow((combination_row(doubletype_indexes(index)).asInstanceOf[Double] - mean_std._1) / mean_std._2, 2))
                      } else {
                        0.0
                      }
                    }).product
                    prob_continuous * prob_discrete
                  } else prob_discrete
                } else {

                  /* To be added. this is the case where Xi itself is continuous data and has parents */
                  //                val discrete_neighbors = neighbors.foldLeft(Array(-1L))((res, vid) => {
                  //                  if (fields_arr(vid.toInt).dataType == IntegerType) res :+ vid
                  //                  else res
                  //                }).filter(value => value != -1L)
                  val discrete_neighbors = neighbors.filter(p => fields_arr(p.toInt).dataType == IntegerType).sorted
                  /* This is the case where Xi has discrete parents */
                  if (!discrete_neighbors.isEmpty) {

                      val index = discrete_neighbors.sorted.map(vid => row(vid.toInt).asInstanceOf[Int]).mkString
                      val ed_index = f._2.indexWhere(p => p._1 == index)
                      val fetchedData = if (ed_index != -1)
                      {
                        f._2(ed_index)
                      } else {
                        ("no_prefix", default_prob, Array((0.0,1.0)))
                      }

                      val mean_dst = fetchedData._3(0)._1
                      val std_dst = fetchedData._3(0)._2
                      val cont_edges = edges.filter(ed => ed.dstId == f._1 && fields_arr(ed.srcId.toInt).dataType != IntegerType)
                    val cont_prob = cont_edges.map(ed => {
                        val mean_src = vertices(ed.srcId.toInt)._2(0)._3(0)._1
                        val std_src = vertices(ed.srcId.toInt)._2(0)._3(0)._2
                        val correlation = ed.attr(ed_index)
                        val S = pow((row(ed.srcId.toInt).asInstanceOf[Double] - mean_src) / std_src, 2) +
                          pow((row(ed.dstId.toInt).asInstanceOf[Double] - mean_dst) / std_dst, 2) -
                          2.0 * correlation * (row(ed.srcId.toInt).asInstanceOf[Double] - mean_src) * (row(ed.dstId.toInt).asInstanceOf[Double] - mean_dst) / (std_src * std_dst)
                        val joint_prob = 1.0 / (2.0 * Pi * std_src * std_dst * sqrt(1.0 - pow(correlation, 2))) * exp(-S / (2.0 * (1.0 - pow(correlation, 2))))
                        joint_prob
                      }).product
                      cont_prob
                  }
                  /* This is the case where Xi has only continuous parents */
                  else {
                    val mean_dst = f._2(0)._3(0)._1
                    val std_dst = f._2(0)._3(0)._2
                    val cont_edges = edges.filter(ed => ed.dstId == f._1 && fields_arr(ed.srcId.toInt).dataType != IntegerType)
                    val cont_prob = cont_edges.map(ed => {
                      val mean_src = vertices(ed.srcId.toInt)._2(0)._3(0)._1
                      val std_src = vertices(ed.srcId.toInt)._2(0)._3(0)._2
                      val correlation = ed.attr(0)
                      val S = pow((row(ed.srcId.toInt).asInstanceOf[Double] - mean_src) / std_src, 2) +
                        pow((row(ed.dstId.toInt).asInstanceOf[Double] - mean_dst) / std_dst, 2) -
                        2.0 * correlation * (row(ed.srcId.toInt).asInstanceOf[Double] - mean_src) * (row(ed.dstId.toInt).asInstanceOf[Double] - mean_dst) / (std_src * std_dst)
                      val joint_prob = 1.0 / (2.0 * Pi * std_src * std_dst * sqrt(1.0 - pow(correlation, 2))) * exp(-S / (2.0 * (1.0 - pow(correlation, 2))))
                      joint_prob
                    }).product

                    cont_prob
                  }
                }
              }
            }
              /* return the product of all Xi|Parents(Xi) */
            ).product.abs
        }
        (true_row(vertexId.toInt).asInstanceOf[Int].toDouble, prob.indexOf(prob.max).toDouble)
      })
    }
    /* END OF CLASS */
  }