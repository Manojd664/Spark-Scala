package com.spark.scala.learnscala.driver

import org.apache.spark.sql.SparkSession
import com.spark.scala.learnscala.wordcount.WordCount
import com.spark.scala.learnscala.dataframe.WaysToCreateDFs
import com.spark.scala.learnscala.dynamicsqlquery.DynamicSQLQueryBuilder
import com.spark.scala.learnscala.dataframeanalysis.DataFrameAnalysis
import com.spark.scala.learnscala.ml.MachineLearniingEx

object Driver extends App {
  
  //Creating Spark Session
  val sparkSession=SparkSession.builder().master("local").appName("LearnScalaSpark").getOrCreate()
  
  //Starting word Count process
  //WordCount.executeWordCount(sparkSession)
  
  //Starting ways to create dataframe process
  //WaysToCreateDFs.executeWaysToCreateDFs(sparkSession)
  
  //Starting dynamic query builder
  //DynamicSQLQueryBuilder.executeDSQLQueryBuilder(sparkSession)
  
  //Starting Data Analysis on structure data
 // DataFrameAnalysis.executeDataFrameAnalysis(sparkSession)
  
  //Starting Machine Learning example
  MachineLearniingEx.executeMachineLearningEx(sparkSession)
}