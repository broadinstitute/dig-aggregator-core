package org.broadinstitute.dig.aggregator.core

import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, ZoneOffset}

import software.amazon.awssdk.services.s3.model.S3Object

object Implicits {
  import scala.language.implicitConversions

  /** Convert from a string to a glob. */
  implicit def stringToGlob(s: String): Glob = Glob(s)

  /** Convert from S3 object to an Input. All times are UTC! */
  implicit def s3ObjToInput(obj: S3Object): Input = {
    Input(obj.key, LocalDateTime.ofInstant(obj.lastModified, ZoneOffset.UTC))
  }

  /** Given an S3 key, extract  */
  final implicit class S3Key(val key: String) extends AnyVal {

    /** Extract just the filename: everything after the last path separator. */
    def basename: String = key.substring(key.lastIndexOf('/') + 1)

    /** Extract everything before the filename: up to and including the last path separator. */
    def dirname: String = key.substring(0, key.lastIndexOf('/') + 1)

    /** Extract the common prefix: up to first wildcard or last path separator. */
    def commonPrefix: String = dirname.indexOf('*') match {
      case n if n >= 0 => dirname.substring(0, n)
      case _           => dirname
    }
  }
}
