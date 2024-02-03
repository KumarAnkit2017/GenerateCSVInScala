package com.generatecsv

import au.com.bytecode.opencsv.CSVWriter
import com.csvschema.Patients

import java.io.FileWriter
import java.sql.Date
import scala.util.Random

object GenerateCSV {

  def writeCsv(outputPath: String,noOfRecords:Integer): Unit = {
    // Create a CSV writer
    val writer = new CSVWriter(new FileWriter(outputPath))


    try {

      writer.writeNext("patient_id", "first_name", "last_name", "gender", "birth_date",
        "city", "provide_id", "allergies", "height", "weight")

      for (patient<- 1 to noOfRecords) {
        val record = Patients(
          patient,
          randomName(5),
          randomName(8),
          randomGender(),
          randomBirthDate(),
          randomName(8),
          randomProvinceId(),
          randomName(10),
          Random.nextInt(50) + 150,
          Random.nextInt(50) + 70
        )

        writer.writeNext(record.patient_id.toString,record.first_name,record.last_name,record.gender.toString,record.birth_date.toString,
          record.city,record.provide_id,record.allergies,record.height.toString,record.weight.toString)
      }



    } finally {
      // Close the writer
      writer.close()
      println(s"Generated $noOfRecords random records and saved to patients_data.csv")
    }
  }

  def randomName(length: Int): String ={
    Random.alphanumeric.take(length).mkString
  }
  def randomGender(): Char =
    {
      if (Random.nextBoolean()) 'M' else 'F'
    }

 def randomProvinceId():String={
   if (Random.nextBoolean()) "ON" else "OF"
 }
  def randomBirthDate(): Date =
    {
      Date.valueOf(s"${Random.nextInt(30) + 1970}-${Random.nextInt(12) + 1}-${Random.nextInt(28) + 1}")
    }





}
