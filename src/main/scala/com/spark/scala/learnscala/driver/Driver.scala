package com.spark.scala.learnscala.driver

import org.apache.spark.sql.SparkSession
import com.spark.scala.learnscala.wordcount.WordCount

object Driver extends App {
  
  //Creating Spark Session
  val sparkSession=SparkSession.builder().master("local").appName("LearnScalaSpark").getOrCreate()
  
  //Starting word Count process
  //WordCount.executeWordCount(sparkSession)
  
  
  
}