package org.broadinstitute.dig.aggregator.core

import software.amazon.awssdk.services.s3.model.S3Object

object Implicits {
  import scala.language.implicitConversions

  /** Implicit conversions. */
  implicit def stringToGlob(s: String): Glob = Glob(s)
  implicit def s3ObjToInput(obj: S3Object): Input = {
    import org.broadinstitute.dig.aws.Implicits.RichS3Object

    // need to strip the e-tag of quotes
    Input(obj.key, RichS3Object(obj).version)
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
