package com.gitbub.ka1904787.Configuration

import com.typesafe.config.{Config, ConfigFactory}

class AppConfiguration(config:Config) {

  def this() {
    this(ConfigFactory.load())
  }

  def csvPath(path: String):String= {
    config.getString(path)
  }

}


