package org.broadinstitute.dig.aggregator.core

import java.time.LocalDateTime
import Implicits._
import org.broadinstitute.dig.aws.S3

/** A run Input is a S3 key and eTag (checksum) pair. */
case class Input(key: String, version: LocalDateTime) {

  /** Basename is the filename portion of the key. */
  lazy val basename: String = key.basename

  /** Prefix is the directory name of the key. */
  lazy val dirname: String = key.dirname
}

/** Companion object with source locations. */
object Input {

  /* An input source an S3 object used to identify all sibling and
   * child objects that can be processed. Typically the key is either
   * "metadata" or "_SUCCESS", but can be a glob.
   *
   * When determining what data needs to be processed, the prefix
   * is used to recursively list all objects in S3 matching key.
   * The ETag of those objects is compared against the ETag of the
   * last time it was processed by the stage.
   */
  case class Source(prefix: String, basename: String, s3BucketOverride: Option[S3.Bucket]) {
    require(prefix.endsWith("/"))
    require(!basename.endsWith("/"))

    /** Pattern matching globs for the prefix and basename of inputs. */
    val prefixGlob: Glob   = Glob(prefix)
    val basenameGlob: Glob = Glob(basename)

    /** Create inputs for all keys in the bucket matching the prefix + basename. */
    def inputs()(implicit context: Context): Seq[Input] = {
      s3BucketOverride.getOrElse(context.s3)
        .ls(prefix.commonPrefix)
        .view
        .map(s3ObjToInput)
        .filter(matches)
        .toSeq
    }

    /** True if an input matches this source. */
    def matches(input: Input): Boolean = {
      basenameGlob.matches(input.basename) && prefixGlob.matches(input.dirname, partial = true)
    }

    /** Extractor method for pattern matching.
      *
      * Allows taking a input and matching the elements of the prefix and
      * basename globs. Wildcard elements are captured and returned in the
      * match. Everything else is an exact match and ignored.
      */
    def unapplySeq(input: Input): Option[List[String]] = {
      prefixGlob.unapplySeq(input.dirname).flatMap { prefixCaptures =>
        basenameGlob.unapplySeq(input.basename).map(prefixCaptures ++ _)
      }
    }
  }

  /** Companion source object for types of input sources. */
  object Source {

    /** Raw inputs match a specific file. */
    def Raw(key: String, s3BucketOverride: Option[S3.Bucket] = None): Source = {
      val (prefix, name) = key.lastIndexOf('/') match {
        case n if n < 0 => throw new Exception(s"Invalid raw Input.Source: $key")
        case n          => key.splitAt(n + 1)
      }

      Source(prefix, name, s3BucketOverride)
    }

    /** Dataset inputs match a prefix to the metadata basename. */
    def Dataset(prefix: String, s3BucketOverride: Option[S3.Bucket] = None): Source =
      Source(prefix, "metadata", s3BucketOverride)

    /** Successful job results match a prefix to the _SUCCESS basename. */
    def Success(prefix: String, s3BucketOverride: Option[S3.Bucket] = None): Source =
      Source(prefix, "_SUCCESS", s3BucketOverride)
  }
}
