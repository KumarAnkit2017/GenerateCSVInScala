package com.generatecsv

import au.com.bytecode.opencsv.CSVWriter
import com.csvschema.Patients

import java.io.FileWriter

object GenerateCSV {

  def writeCsv(outputPath: String): Unit = {
    // Create a CSV writer
    val writer = new CSVWriter(new FileWriter(outputPath))

    try {

      writer.writeNext("patient_id", "first_name", "last_name", "gender", "birth_date",
        "city", "provide_id", "allergies", "height", "weight")

    } finally {
      // Close the writer
      writer.close()
    }
  }

}
