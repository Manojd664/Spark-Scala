package com.spark.scala.learnscala.wordcount

import org.apache.spark.sql.SparkSession

object WordCount{
  
  def executeWordCount(sparkSession:SparkSession)
  {
    println("starting word count process ")
    
    //Reading input file and creating rdd with no of partitions 5
    val bookRDD=sparkSession.sparkContext.textFile("inputFiles//wordcount//metamorphosis.txt", 4)
    
    //Regex to clean text
    val pat = """[^\w\s\$]"""
    val cleanBookRDD=bookRDD.map(line=>line.replaceAll(pat, ""))
    
    val wordsRDD=cleanBookRDD.flatMap(line=>line.split(" "))
    
    val wordMapRDD=wordsRDD.map(word=>(word->1))
    
    val wordCountMapRDD=wordMapRDD.reduceByKey(_+_)
    
    wordCountMapRDD.saveAsTextFile("outputfiles//wordCount//wordcount.txt")
    
    
    
  }
  
}