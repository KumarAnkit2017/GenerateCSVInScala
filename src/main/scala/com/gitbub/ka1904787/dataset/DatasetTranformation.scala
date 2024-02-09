package com.gitbub.ka1904787.dataset

import com.gitbub.ka1904787.schemas.{PatientProvince, Patients, PatientsFirstName, PatientsName, ProvinceName}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object DatasetTranformation {

  def main(args: Array[String]): Unit = {


    ///Entry point of spark application and my master is running on locally
    val spark = SparkSession.builder().appName("Dataset Transformation").master("local").getOrCreate();

    ////Csv Patient Path
    val csvPatientPath = "E:\\Scala\\PatientsRecords.csv"

    ////Csv Patient Path
    val csvProvincePath = "E:\\Scala\\province_names.csv"

    //Loading Patient CSV File in Dataset
    import spark.implicits._
    val loadPateintInDataSet: Dataset[Patients] = spark.read.format("csv").schema(Encoders.product[Patients].schema).option("header","true").csv(csvPatientPath).as[Patients]

    val loadProvinceInDataSet: Dataset[ProvinceName] = spark.read.format("csv").schema(Encoders.product[ProvinceName].schema).option("header","true").csv(csvProvincePath).as[ProvinceName]


    //show Patient in form of Dataset
    loadPateintInDataSet.show();

    //show Province Data in form of Dataset
    loadProvinceInDataSet.show()

    //1. Show first name and last name of patients whose gender is M
    //loadCsvInDataSet.filter(filterByMale=>filterByMale.gender=="M").select($"firstName", $"lastName",$"gender")
      //.as[(String, String,String)].show()
    loadPateintInDataSet.filter(filterByMale=>filterByMale.gender=="M").map(patients=>PatientsName(patients.firstName,patients.lastName)).show()

    //2. Show first name and last name of patients who does not have allergies. (null)
    loadPateintInDataSet.filter(noAllergiesPatients=>noAllergiesPatients.allergies==null).map(patients=>PatientsName(patients.firstName,patients.lastName)).show()

    //3. SELECT first_name FROM patients where first_name like 'c%'
    loadPateintInDataSet.filter(patientNameStartWithC=>patientNameStartWithC.firstName.startsWith("c")).map(patients=>PatientsFirstName(patients.firstName)).show()

    //4. SELECT first_name,last_name FROM patients where weight between 100 and 120
    loadPateintInDataSet.filter(patientWeight=>(patientWeight.weight>=100 && patientWeight.weight<=120)).map(patients=>PatientsName(patients.firstName,patients.lastName)).show()


    //5. Update the patients table for the allergies column. If the patient's allergies is null then replace it with 'NKA'
    loadPateintInDataSet.map(patient => {
      val allergies: String =
        if (patient.allergies == null) "NKA"
        else patient.allergies
      Patients(patient.patientId, patient.firstName, patient.lastName, patient.gender,
        patient.birthDate, patient.city, patient.provinceId, allergies, patient.height, patient.weight)
    }).show()

    //6. SELECT concat(first_name,' ',last_name) as full_name FROM patients
    loadPateintInDataSet.map(fullName=>PatientsFirstName(fullName.firstName+" "+fullName.lastName)).show()

    //7. Show first name, last name, and the full province name of each patient.
    val joinPatientWithProvince= loadPateintInDataSet.joinWith(loadProvinceInDataSet,loadPateintInDataSet("provinceId")===loadProvinceInDataSet("provinceId")).as("patient")
    joinPatientWithProvince.map(patientProvince=>PatientProvince(patientProvince._1.firstName,patientProvince._1.lastName,patientProvince._2.provinceName)).show()





  }

}
