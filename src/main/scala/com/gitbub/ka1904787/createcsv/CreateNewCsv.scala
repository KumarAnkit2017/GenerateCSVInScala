package com.gitbub.ka1904787.createcsv

import java.io.{BufferedReader, FileReader}
import scala.util.{Random, Try, Using}

object CreateNewCsv {

  def main(args: Array[String]): Unit = {

    val randamDataGenerator=new RandamDataGenerator();
    randamDataGenerator.writeCsv("E:\\Scala\\PatientsRecords.csv", 10000);
  }

}
