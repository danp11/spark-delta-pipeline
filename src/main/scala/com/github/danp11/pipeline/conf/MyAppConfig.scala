package com.github.danp11.pipeline.conf

import com.typesafe.config.{Config, ConfigFactory}

abstract class MyAppConfig {

  private val base = ConfigFactory.load.getConfig("myapp")
  private val environment = Option(System.getenv("MYAPP_ENV")).getOrElse("dev")

  protected def load(): Config = {

   // val config = base.getConfig(subConfig)

    if (base.hasPath(environment))
      base.getConfig(environment).withFallback(base)

    base
  }
}
