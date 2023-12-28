# SparkBN# GraduateThesis
This is the implementation presented during my Graduation Thesis in the Computer Engineering &amp; Informatics Department at University of Patras. The Implementation was done in `Scala` programming language, using the `Apache Spark Framework`. 

# Abstract
---
Every year the volume of information is growing at a high rate. Existing computer systems are reaching the limits of handling such data, and as a result, a more modern approach is required in the field of data science to deal efficiently with the problems of the new era. Distributed systems, offer such a modern approach, and as such, more and more machine learning models for automatic decision-making are being transfered to distributed environments such as that of Apache Spark. In the work presented in this thesis, a new classification model based on Bayesian networks is proposed, which utilizes the distributed environment of Apache Spark and is capable of handling continuous features. This model is experimentally evaluated with corresponding classical Bayesian Network models as well as different classifiers, in order to show that this model can be competitive and complete in this new era of information. 

The aim of this thesis is to create a modern and flexible machine learning model based on the Discrete Bayesian Network model for Classification, which implementation, utilizes the Apache Spark distributed environment for Big Data management and can properly handle continuous features.

# Implementation
---
DBN (Distributed Bayesian Network), is built on top of the Spark SQL and GraphX libraries, and therefore, has a direct dependency on both. The way that resource allocation is achieved in the systems is through intermediate information stored in the form of Dataframes, and the final information is collected, brokenly, at the driver and sent in chunks over the distributed graph. This graph, is the essence of the model as it is the tool that the model uses to train and later, make inferences based on queries. It contains the initial information, and after training is complete, it contains the final information.

In this way, the model exploits the structure of Spark's RDDs, and the parallelization points, guarantee the efficient use of resources during computation. Also, through Spark we exploit the possibility of writing data to the random access memory (RAM) of the worker nodes, in order to avoid transferring temporary data to the main memory of the systems, which would add significant latency. The model performs, many computations, where multiple intermediate data are generated, needed to produce the final information. Therefore, by leveraging the use of RAM we achieve lower performance times during the creation of intermediate data.

In the case of intermediate data, we use the dataframe structure to store and interact with information in a distributed manner. For discrete vertices, we compute the intermediate information as conditional probabilities for each state of the random variables and store it in the Dataframe as a temporary storage medium until all the necessary conditional probabilities for the vertex of interest are computed. 
For continuous vertices, we compute the mean and standard deviation relevant to the vertex via an existing Dataframe.
In the case of the final results, we take advantage of the fact that the intermediate data relevant for generating the final information is already in a condensed form. For discrete vertices we now have a simple table that has a size, which depends on the states of the parents, if they exist, and the states of the vertex itself. For example, if a discrete vertex we are studying has 3 possible categories, and has a parent that is also discrete and has 2 possible states, then the size of the CPT would be 3𝑥2, which for a dataframe, that means 6 columns, with just one line. Therefore, we can now gather the information via Spark Actions, such as collect, in the driver and from there transfer the result into the graph, to the corresponding vertex. The reason is, precisely because instead of having, possibly, millions of data, we now have just 6 which do not add much delay during the collect process in the driver. Spark does not allow direct transfer of a distributed structure to another distributed structure, so the collection stage in the driver is deemed necessary.

The above parallelization points theoretically guarantee the scalability of the model to Big Data datasets, as well as their better time response during training. The scalability, is mainly due to the management of computation and storage in a distributed way, which means that, the increase of the workload in the system, will not dramatically affect the response of the model, as it depends on the resources available in each computing system of the distributed model. The time improvement, is purely due to the number and quality of resources we have available. Increasing the number of worker nodes, implies, greater breakage into data blocks, i.e. better resource allocation, and hence time improvement. The processing power available to each worker also helps a lot.

# Run Spark Application
To run the code you will need to have `spark` version 3.x and above. The `build.sbt` file is provided for quick packaging through the `sbt package` command. You can either use `spark-submit` or `spark-shell` to run the Spark app, however it is recommended to use `spark-shell` as it is more stable in this thesis current form.

You may use the following command to run the example i provide in this repository


Packaging the build into a jar file (Within BayesThesis directory)
---
    sbt package
---
After the packaging is complete run the example code using the following command
---
    spark-shell --master local[*] --jars bayesthesis_2.12-1.0.jar -i src/main/scala/BayesianNetwork.scala -i src/main/scala/workshee.scala
---

For any further clarifications i would be happy to assist. Feel free to contact me in akarepis@ceid.upatras.gr