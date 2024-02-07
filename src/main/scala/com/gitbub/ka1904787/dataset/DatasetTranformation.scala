package com.gitbub.ka1904787.dataset

import com.gitbub.ka1904787.schemas.{Patients, PatientsFirstName, PatientsName}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object DatasetTranformation {

  def main(args: Array[String]): Unit = {


    ///Entry point of spark application and my master is running on locally
    val spark = SparkSession.builder().appName("Dataset Transformation").master("local").getOrCreate();

    ////Csv Path
    val csvPath = "E:\\Scala\\PatientsRecords.csv"

    //Loading Patient CSV File in Dataset
    import spark.implicits._
    val loadCsvInDataSet: Dataset[Patients] = spark.read.format("csv").schema(Encoders.product[Patients].schema).csv(csvPath).as[Patients]

    //show data in form of Dataset
    loadCsvInDataSet.show();

    //1. Show first name and last name of patients whose gender is M
    //loadCsvInDataSet.filter(filterByMale=>filterByMale.gender=="M").select($"firstName", $"lastName",$"gender")
      //.as[(String, String,String)].show()
    loadCsvInDataSet.filter(filterByMale=>filterByMale.gender=="M").map(patients=>PatientsName(patients.firstName,patients.lastName)).show()

    //2. Show first name and last name of patients who does not have allergies. (null)
    loadCsvInDataSet.filter(noAllergiesPatients=>noAllergiesPatients.allergies==null).map(patients=>PatientsName(patients.firstName,patients.lastName)).show()

    //3. SELECT first_name FROM patients where first_name like 'c%'
    loadCsvInDataSet.filter(patientNameStartWithC=>patientNameStartWithC.firstName.startsWith("c")).map(patients=>PatientsFirstName(patients.firstName)).show()

    //4. SELECT first_name,last_name FROM patients where weight between 100 and 120
    loadCsvInDataSet.filter(patientWeight=>(patientWeight.weight>=100 && patientWeight.weight<=120)).map(patients=>PatientsName(patients.firstName,patients.lastName)).show()


    //5. Update the patients table for the allergies column. If the patient's allergies is null then replace it with 'NKA'
    loadCsvInDataSet.map(patient => {
      val allergies: String =
        if (patient.allergies == null) "NKA"
        else patient.allergies
      Patients(patient.patientId, patient.firstName, patient.lastName, patient.gender,
        patient.birthDate, patient.city, patient.provideId, allergies, patient.height, patient.weight)
    }).show()






  }

}
