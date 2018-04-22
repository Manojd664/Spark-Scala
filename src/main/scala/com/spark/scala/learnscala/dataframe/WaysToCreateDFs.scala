/*This object contains different ways to create DataFrames */
//https://medium.com/@mrpowers/manually-creating-spark-dataframes-b14dae906393
package com.spark.scala.learnscala.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row

object WaysToCreateDFs {
  
  def executeWaysToCreateDFs(sparkSession:SparkSession)
  {
    //1.Creating Dataframe from List to Rows
    val list=List(Row("Manoj","Kumar",24,"Developer"),Row("Alice","Pot",23,"Tester"),Row("Bob","Trump",25,"Manager"))
    
    val schema=List(StructField("fname",StringType,true),StructField("lname",StringType,true),StructField("age",IntegerType,true),StructField("designation",StringType,true))
    
    val dfFromList=sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(list), StructType(schema))
    
    println("Dataframe from list of Rows")
    dfFromList.show
    
    //2.Creating Dataframe from csv file
    
    val dfFromCSV=sparkSession.read.format("csv").option("header", "true").load("Inputfiles//dataframe//emp.csv")
    
    println("Dataframe from csv file")
    dfFromCSV.show()
  }
}