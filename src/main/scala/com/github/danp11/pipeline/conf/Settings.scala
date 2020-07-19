package com.github.danp11.pipeline.conf

import com.typesafe.config.{ConfigFactory}

object Settings {

  private val base = ConfigFactory.load.getConfig("myapp")
  private val environment = if (System.getenv("ENVIRONMENT") == null) "dev"
  else System.getenv("ENVIRONMENT")

  val rootPath: String = load("pipeline").getString("rootPath")

  private def load(setting: String) = {
    val config = base.getConfig(setting)
    config.getConfig(environment).withFallback(config)
  }
}
