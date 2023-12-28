# SparkBN 

Until now, Apache Spark remains one of the most widely used engines for handling Big Data, offering a range of tools for various large-scale data processing tasks. However, despite the many methods available in MLlib and other third-party libraries, some are still missing, like the Bayesian Network (BN) models. BNs are graphical models that use a graph to show probabilistic relationships, storing information in the vertices of the graph. Each vertex represents a dataset feature, and the directed edges show the statistical dependence between a vertex and its parent(s). The graph must be a Directed Acyclic Graph (DAG). Existing implementations of BNs for Apache Spark, such as the one by [Arias et al (2017)](https://github.com/jacintoArias/spark-bnc), were developed for older versions and can't take advantage of the improvements in newer versions.

So, the goal of SparkBN is to offer a modern and adaptable method for efficiently learning parameters. It works with any user-provided DAG, showing the dependencies among a dataset's features. SparkBN fully utilizes the distributed environment of Apache Spark 3, making it scalable and capable of handling large datasets. SparkBN doesn't include a structure learning component because its primary focus is on providing a general model that can handle any input DAG. This flexibility extends to SparkBN's ability to deal with both categorical and continuous features, making it suitable for hybrid BNs. The implementation and parallelization heavily rely on two core Apache Spark libraries: GraphX, a distributed graph processing library, and SparkSQL, which offers Dataframes for easier and faster data manipulation.

# Run SparkBN

To run the code you will need to have `spark` version 3.x and above. The `build.sbt` file is provided for quick packaging through the `sbt package` command. 

## Build and Package the SparkBN jar

    sbt package