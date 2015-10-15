package com.ulb.code.wit.util

import java.io.File

import com.typesafe.config.{ ConfigFactory, Config }

import scala.util.{ Failure, Success, Try }

object ConfigUtil {
  var config: Config = ConfigFactory.load

  def load(configFile: String): Unit =
    {
      Try(ConfigFactory.parseFile(new File(configFile))) match {
        case Success(s) => config = s
        case Failure(e) => config = ConfigFactory.load
      }
    }

  def get[T](key: String, default: T): T =
    {
      Try(config.getAnyRef(key).asInstanceOf[T]) match {
        case Success(s) => s
        case Failure(e) => default
      }
    }

  def get[T](key: String): T =
    {
      config.getAnyRef(key).asInstanceOf[T]
    }

}
