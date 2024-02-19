package com.gitbub.ka1904787.schemas

import java.sql.Date

case class Patients(
                     patientId: Integer,
                     firstName: String,
                     lastName: String,
                     gender: String,
                     birthDate: Date,
                     city: String,
                     provinceId: String,
                     allergies: String,
                     height: Integer,
                     weight: Integer

                   )

case class PatientsName(
                         firstName: String,
                         lastName: String,

                       )

case class PatientsFirstName(
                              firstName: String,
                            )

case class PatientsDOBYears(years: Int)

case class PatientsNameByGroup(firstName: String,
                               noOfRecords:Int)

case class PatientsNameHaving6Letter(patientId: Int,
                               firstName:String)

case class PatientsGroupByWeight(wight:Int,patients_groups: Int)





