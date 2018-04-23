package com.spark.scala.learnscala.ml

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.RFormula
object MachineLearniingEx {
   
  def executeMachineLearningEx(sparkSession:SparkSession)
  {
    
    //Create Dataframe of train data
    val trainDf=sparkSession.read.format("csv").option("header", "true").load("Inputfiles//dataAnalysisAndML//train.csv")
    
    //Creating Dataframe of test data
    val testDf=sparkSession.read.format("csv").option("header", "true").load("Inputfiles//dataAnalysisAndML//test.csv")
    
    //I am going to create simple model with 3 dependent and 1 target
    
    val df1 = trainDf.select("User_ID","Occupation","Marital_Status","Purchase")
    
    val formula = new RFormula().setFormula("Purchase ~ User_ID+Occupation+Marital_Status").setFeaturesCol("features").setLabelCol("label")
    
    val train = formula.fit(df1).transform(df1)
     
    val df2=testDf.select("User_ID","Occupation","Marital_Status")
    val test=formula.fit(df2).transform(df2)
    
    val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val lrModel = lr.fit(train)
    
    val pred=lrModel.transform(test)
    
    pred.show(10)
    
    //Sample Output:
    /*
     +-------+----------+--------------+--------------------+------------------+
|User_ID|Occupation|Marital_Status|            features|        prediction|
+-------+----------+--------------+--------------------+------------------+
|1000004|         7|             1|(5911,[5886,5892]...|2803.6194984730428|
|1000009|        17|             0|(5911,[2604,5894,...|  3614.74139674703|
|1000010|         1|             1|(5911,[528,5893],...|2740.7904882970583|
|1000010|         1|             1|(5911,[528,5893],...|2740.7904882970583|
|1000011|         1|             0|(5911,[1864,5893,...|2131.5435796725797|
|1000013|         1|             1|(5911,[2711,5893]...| 3222.530638022158|
|1000013|         1|             1|(5911,[2711,5893]...| 3222.530638022158|
|1000013|         1|             1|(5911,[2711,5893]...| 3222.530638022158|
|1000015|         7|             0|(5911,[1590,5892,...|2170.6685340471913|
|1000022|        15|             0|(5911,[815,5904,5...|3470.8435206825234|
+-------+----------+--------------+--------------------+------------------+*/
  }
}