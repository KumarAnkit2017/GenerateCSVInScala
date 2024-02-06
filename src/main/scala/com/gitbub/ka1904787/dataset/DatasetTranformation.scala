package com.gitbub.ka1904787.dataset

import com.gitbub.ka1904787.schemas.Patients
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object DatasetTranformation {

  def main(args: Array[String]): Unit = {


    ///Entry point of spark application and my master is running on locally
    val spark = SparkSession.builder().appName("Dataset Transformation").master("local").getOrCreate();

    ////Csv Path
    val csvPath = "E:\\Scala\\PatientsRecords.csv"

    //Loading Patient CSV File in Dataset
    import spark.implicits._
    val loadCsvInDataSet: Dataset[Patients] = spark.read.format("csv").schema(Encoders.product[Patients].schema).option("header", "true").csv(csvPath).as[Patients]

    //show data in form of Dataset
    loadCsvInDataSet.show();

    //1. Show first name and last name of patients whose gender is M
    loadCsvInDataSet.filter(filterByMale=>filterByMale.gender=="M").select($"firstName", $"lastName",$"gender")
      .as[(String, String,String)].show()

    loadCsvInDataSet.filter(filterByMale=>filterByMale.gender=="M").foreach(pateint=>println(pateint.firstName+" | "+pateint.lastName+" | "+pateint.gender))



  }

}
