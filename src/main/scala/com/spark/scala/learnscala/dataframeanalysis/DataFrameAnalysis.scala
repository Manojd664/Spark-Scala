package com.spark.scala.learnscala.dataframeanalysis

import org.apache.spark.sql.SparkSession

object DataFrameAnalysis {
  
  def executeDataFrameAnalysis(sparkSession:SparkSession)
  {
    
    println("Starting Data Analysis on structure data")
    //Creating DataFrame from existing csv file
    val trainDf=sparkSession.read.format("csv").option("header", "true").load("Inputfiles//dataAnalysisAndML//train.csv")
    
    trainDf.show(5)
    
    /*First 5 rows of dataframe
 +-------+----------+------+----+----------+-------------+--------------------------+--------------+------------------+------------------+------------------+--------+
|User_ID|Product_ID|Gender| Age|Occupation|City_Category|Stay_In_Current_City_Years|Marital_Status|Product_Category_1|Product_Category_2|Product_Category_3|Purchase|
+-------+----------+------+----+----------+-------------+--------------------------+--------------+------------------+------------------+------------------+--------+
|1000001| P00069042|     F|0-17|        10|            A|                         2|             0|                 3|              null|              null|    8370|
|1000001| P00248942|     F|0-17|        10|            A|                         2|             0|                 1|                 6|                14|   15200|
|1000001| P00087842|     F|0-17|        10|            A|                         2|             0|                12|              null|              null|    1422|
|1000001| P00085442|     F|0-17|        10|            A|                         2|             0|                12|                14|              null|    1057|
|1000002| P00285442|     M| 55+|        16|            C|                        4+|             0|                 8|              null|              null|    7969|
+-------+----------+------+----+----------+-------------+--------------------------+--------------+------------------+------------------+------------------+--------+
*/
    
    
    
    //Displaying the schema of Dataframe
    trainDf.printSchema()
    
    
    
    /* Output: 
     root
 |-- User_ID: string (nullable = true)
 |-- Product_ID: string (nullable = true)
 |-- Gender: string (nullable = true)
 |-- Age: string (nullable = true)
 |-- Occupation: string (nullable = true)
 |-- City_Category: string (nullable = true)
 |-- Stay_In_Current_City_Years: string (nullable = true)
 |-- Marital_Status: string (nullable = true)
 |-- Product_Category_1: string (nullable = true)
 |-- Product_Category_2: string (nullable = true)
 |-- Product_Category_3: string (nullable = true)
 |-- Purchase: string (nullable = true)
 
 */
    
    //Displaying number of records in train table or trainDf
    
    println("Total Records in trainDF="+trainDf.count)
    
    //Output: Total Records in trainDF=550068
    
    //getting all column names
    val columnList=trainDf.columns.toList
    println("Column List: "+columnList)
    
    //Sample Output: Column List: List(User_ID, Product_ID, Gender, Age, Occupation, City_Category, Stay_In_Current_City_Years, Marital_Status, Product_Category_1, Product_Category_2, Product_Category_3, Purchase)
    
    //Now write some queries on Dataframe
    val uIDAndGender=trainDf.select(trainDf.col("User_ID"),trainDf.col("Gender"))
    uIDAndGender.show(5)
    
    //Sample Output: 
    /*
     +-------+------+
|User_ID|Gender|
+-------+------+
|1000001|     F|
|1000001|     F|
|1000001|     F|
|1000001|     F|
|1000002|     M|
+-------+------+*/
    
    //Applying filter conditions on Dataframe 
    
    val purchase=trainDf.select(trainDf.col("User_ID"),trainDf.col("Purchase")).filter("Purchase >10000")
    purchase.show(5)
    
    /*
     * Sample Output: 
     +-------+--------+
|User_ID|Purchase|
+-------+--------+
|1000001|   15200|
|1000003|   15227|
|1000004|   19215|
|1000004|   15854|
|1000004|   15686|
*/
    
    //If you are not able to remember spark functions for dataframe and you are good at SQL then you can also apply your knowledge of SQL
    trainDf.createOrReplaceTempView("train")
    
    val df=sparkSession.sql("select User_ID,Purchase from train where Purchase>10000")
    df.show(5)
    
    /*
     +-------+--------+
|User_ID|Purchase|
+-------+--------+
|1000001|   15200|
|1000003|   15227|
|1000004|   19215|
|1000004|   15854|
|1000004|   15686|
+-------+--------+
*/
  }
}