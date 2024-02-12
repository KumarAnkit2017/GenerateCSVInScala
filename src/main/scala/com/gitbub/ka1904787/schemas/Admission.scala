package com.gitbub.ka1904787.schemas

import java.sql.Date

case class Admission(
                      patientId: Int,
                      admissionDate: String,
                      dischargeDate: String,
                      diagnosis: String,
                      attendingDoctorId: Int
                    )

case class AdmissionAndPatient(
                                patientId: Int,
                                firstName: String,
                                lastName: String,
                                diagnosis: String
                              )



