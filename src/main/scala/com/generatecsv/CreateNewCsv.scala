package com.generatecsv

object CreateNewCsv {

  def main(args:Array[String]): Unit = {

    GenerateCSV.writeCsv("E:\\Scala\\PatientsRecords.csv");
    println("CSV Created Succesfully");
  }

}
