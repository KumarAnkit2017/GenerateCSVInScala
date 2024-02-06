package com.gitbub.ka1904787.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DataframeTransformation {

  def main(args: Array[String]): Unit = {
    ///Entry point of spark application and my master is running on locally
    val spark = SparkSession.builder().appName("DataFrameTransformation").master("local").getOrCreate();

    //Csv Path
    val csvPath = "E:\\Scala\\PatientsRecords.csv"

    //Loading Patient CSV File in DataFrame
    val loadCsvInDataFrame = spark.read.format("csv").option("header", "true").csv(csvPath).toDF()

    //show data in form of dataframe
    loadCsvInDataFrame.show()

    //1. Show first name and last name of patients whose gender is M
    loadCsvInDataFrame.selectExpr("first_name","last_name","gender").where("gender ='M' ").show()


  }

}
