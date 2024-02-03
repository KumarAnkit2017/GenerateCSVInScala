package com.csvschema
import java.sql.Date

case class Patients(
                   patient_id: Integer,
                   first_name:String,
                   last_name:String,
                   gender:Char,
                   birth_date:Date,
                   city:String,
                   provide_id:String,
                   allergies:String,
                   height:Integer,
                   weight:Integer

                   )
