package com.gitbub.ka1904787.dataframe

import com.gitbub.ka1904787.schemas.{PatientProvince, Patients, PatientsFirstName, PatientsName, ProvinceName}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.{aggregate, coalesce, col, concat, lit}

object DataframeTransformation {

  def main(args: Array[String]): Unit = {
    ///Entry point of spark application and my master is running on locally
    val spark = SparkSession.builder().appName("DataFrameTransformation").master("local").getOrCreate();

    ////Csv Patient Path
    val csvPatientPath = "E:\\Scala\\PatientsRecords.csv"

    ////Csv Patient Path
    val csvProvincePath = "E:\\Scala\\province_names.csv"

    //Loading Patient CSV File in Dataset
    import spark.implicits._
    val loadPateintInDataFrame = spark.read.format("csv").schema(Encoders.product[Patients].schema).option("header","true").csv(csvPatientPath)

    val loadProvinceInDataFrame = spark.read.format("csv").schema(Encoders.product[ProvinceName].schema).option("header","true").csv(csvProvincePath)


    //show Patient in form of Dataset
    loadPateintInDataFrame.show();

    //show Province Data in form of Dataset
    loadProvinceInDataFrame.show()

    //1. Show first name and last name of patients whose gender is M
    loadPateintInDataFrame.select("firstName","lastName","gender").where("gender ='M' ").show()

    //2. Show first name and last name of patients who does not have allergies. (null)
    loadPateintInDataFrame.select("firstName","lastName").where("allergies is null").show()

    //3. SELECT first_name FROM patients where first_name like 'c%'
    loadPateintInDataFrame.select("firstName").where("firstName like 'c%' ").show()


    //4. SELECT first_name,last_name FROM patients where weight between 100 and 120
    loadPateintInDataFrame.select("firstName","lastName").where(" weight between 100 and 120").show()



    //5. Update the patients table for the allergies column. If the patient's allergies is null then replace it with 'NKA'
    loadPateintInDataFrame.withColumn("allergies", coalesce(col("allergies"), lit("NKA"))).show()

    //6. SELECT concat(first_name,' ',last_name) as full_name FROM patients
     loadPateintInDataFrame.select(concat(col("firstName"),lit(" "), col("lastName")).as("fullName")).show()

    //7. Show first name, last name, and the full province name of each patient.
    loadPateintInDataFrame.join(loadProvinceInDataFrame,loadPateintInDataFrame("provinceId")===loadProvinceInDataFrame("provinceId")).select("firstName","lastName","provinceName").show()



9
  }

}
