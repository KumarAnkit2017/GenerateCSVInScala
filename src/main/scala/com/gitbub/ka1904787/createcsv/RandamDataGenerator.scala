package com.gitbub.ka1904787.createcsv

import com.gitbub.ka1904787.schemas.Patients
import com.opencsv.CSVWriter

import java.io.FileWriter
import java.sql.Date
import scala.util.{Random, Using}

class RandamDataGenerator {

  val dataList = List("AB", "BC", "MB", "NB", "NL", "NT", "NS", "NU")


  def writeCsv(outputPath: String, noOfRecords: Integer): Unit = {

    Using(new CSVWriter(new FileWriter(outputPath))) { writer =>

      writer.writeNext(Array("patientId", "firstName", "lastName", "gender", "birthDate",
        "city", "provinceId", "allergies", "height", "weight"))

      for (patient <- 1 to noOfRecords) {
        val record =Patients(
          patient,
          randomName(5),
          randomName(8),
          randomGender(),
          randomBirthDate(),
          randomName(8),
          randomProvinceId(6,dataList),
          randomAllergies(10),
          Random.nextInt(50) + 150,
          Random.nextInt(50) + 70
        )
        writer.writeNext(record.productIterator.map(eachElement=>eachElement.toString).toArray)
      }
      println(s"Generated $noOfRecords random records and saved to patients_data.csv")
    }



      // Create a CSV writer
      //val writer = new CSVWriter(new FileWriter(outputPath))


      /* try {

      writer.writeNext("patient_id", "first_name", "last_name", "gender", "birth_date",
        "city", "provide_id", "allergies", "height", "weight")

      for (patient <- 1 to noOfRecords) {
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

        writer.writeNext(record.patient_id.toString, record.first_name, record.last_name, record.gender.toString, record.birth_date.toString,
          record.city, record.provide_id, record.allergies, record.height.toString, record.weight.toString)
      }


    } finally {
      // Close the writer
      writer.close()
      println(s"Generated $noOfRecords random records and saved to patients_data.csv")
    }
  }*/
      // Function to generate random name of a given length
      def randomName(length: Int): String = {
        Random.alphanumeric.take(length).mkString
      }

    // Function to generate random gender ('M' or 'F')
      def randomGender(): String = {
        if (Random.nextBoolean()) "M" else "F"
      }

    // Function to generate a random province ID
    def randomProvinceId(len: Int, dataList: List[String]): String = {
      val random = new Random
      var province: String = new String();
      for (s <- 1 to len) {
        province= dataList(random.nextInt(s))
      }
      province
    }

    def randomAllergies(length:Int): String = {
      if (Random.nextBoolean()){Random.alphanumeric.take(length).mkString} else{""}
    }

    // Function to generate a random birth date
      def randomBirthDate(): Date = {
        Date.valueOf(s"${Random.nextInt(30) + 1970}-${Random.nextInt(12) + 1}-${Random.nextInt(28) + 1}")
      }

  }
}
