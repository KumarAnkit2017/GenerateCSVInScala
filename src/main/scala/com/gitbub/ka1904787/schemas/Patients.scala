package com.gitbub.ka1904787.schemas

import java.sql.Date

case class Patients(
                     patientId: Integer,
                     firstName: String,
                     lastName: String,
                     gender: String,
                     birthDate: Date,
                     city: String,
                     provideId: String,
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


