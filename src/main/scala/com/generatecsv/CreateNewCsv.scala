package com.generatecsv

import scala.util.Random

object CreateNewCsv {

  def main(args:Array[String]): Unit = {

    GenerateCSV.writeCsv("E:\\Scala\\PatientsRecords.csv",10000);

  }

}
