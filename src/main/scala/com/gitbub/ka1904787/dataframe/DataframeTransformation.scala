package com.gitbub.ka1904787.dataframe

import com.gitbub.ka1904787.schemas.{Patients, PatientsFirstName, PatientsName}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, lit}

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
    loadCsvInDataFrame.select("first_name","last_name","gender").where("gender ='M' ").show()



    //2. Show first name and last name of patients who does not have allergies. (null)
    loadCsvInDataFrame.select("first_name","last_name").where("allergies is null").show()

    //3. SELECT first_name FROM patients where first_name like 'c%'
    loadCsvInDataFrame.select("first_name").where("first_name like 'c%' ").show()


    //4. SELECT first_name,last_name FROM patients where weight between 100 and 120
    loadCsvInDataFrame.select("first_name","last_name").where(" weight between 100 and 120").show()



    //5. Update the patients table for the allergies column. If the patient's allergies is null then replace it with 'NKA'
    loadCsvInDataFrame.withColumn("allergies", coalesce(col("allergies"), lit("NKA"))).show()



9
  }

}
