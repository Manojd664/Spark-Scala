package com.spark.scala.learnscala.dynamicsqlquery

import org.apache.spark.sql.SparkSession
import java.util.Scanner
import java.io.FileReader

object DynamicSQLQueryBuilder {
  
  def executeDSQLQueryBuilder(sparkSession:SparkSession) 
  {
    val scan=new Scanner(new FileReader("Inputfiles//dynamicsqlquerybuilder//tableandfields.txt"))
    var queryMap=Map[String,String]()
    
    while(scan.hasNext())
    {
      val str=scan.next().split("=")
      queryMap=queryMap+(str(0).trim->str(1).trim)
    }
    
    val inptdf=sparkSession.read.format("csv").option("header", "true").load("Inputfiles//dynamicsqlquerybuilder//emp.csv")
    
    inptdf.createOrReplaceTempView(queryMap.getOrElse("src_table", "please provide source table").trim)
    
    val srcFields=queryMap.getOrElse("src_fields", "Please provide source fields name").split(",").toList
    val tgtFields=queryMap.getOrElse("tgt_fields", "Please provide target fields name").split(",").toList
    
    var query="select "
    
    for(index<- 0 until srcFields.length)
    {
      query=query+srcFields(index).trim+" as "+tgtFields(index).trim+", "
    }
    
    query=query.slice(0,query.lastIndexOf(","))
    
    
    query=query +" from "+queryMap("src_table")
    
    println(query) 
    
    val dfAfterQuery=sparkSession.sql(query)
    
    dfAfterQuery.show()
    
  }
}