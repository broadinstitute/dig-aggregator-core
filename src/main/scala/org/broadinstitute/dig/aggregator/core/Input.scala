package org.broadinstitute.dig.aggregator.core

import org.broadinstitute.dig.aws.AWS

/** A run Input is a S3 key and eTag (checksum) pair. */
case class Input(key: String, eTag: String) {

  /** It's common to split an S3 key by path separator. */
  lazy val splitKeys: Array[String] = key.split('/')

  /** Basename is the actual filename of the key. */
  lazy val basename: String = splitKeys.last

  /** Prefix is the directory name of the key. */
  lazy val prefix: String = splitKeys.dropRight(1).mkString("/")
}

/** Companion Input object for sources. */
object Input {
  import org.broadinstitute.dig.aws.Implicits._

  /* An input source an S3 object used to identify all sibling and
   * child objects that can be processed. Typically the key is either
   * "metadata" or "_SUCCESS", but can be a glob.
   *
   * When determining what data needs to be processed, the prefix
   * is used to recursively list all objects in S3 matching key.
   * The ETag of those objects is compared against the ETag of the
   * last time it was processed by the stage.
   */
  sealed trait Source {
    val prefix: String

    /** Returns true if the key matches the source. */
    def matches(key: String): Boolean

    /** Fetch all the keys matching the input and their e-tags. */
    def objects: Seq[Input] = {
      Context.current.aws
        .ls(prefix)
        .filter(obj => matches(obj.key))
        .map(obj => Input(obj.key, obj.eTagStripped))
    }
  }

  /** Companion source object for types of input sources. */
  object Source {

    /** Dataset inputs. */
    final case class Dataset(prefix: String) extends Source {
      override def matches(key: String): Boolean = key.endsWith("/metadata")
    }

    /** SUCCESS spark job results. */
    final case class Success(prefix: String) extends Source {
      override def matches(key: String): Boolean = key.endsWith("/_SUCCESS")
    }

    /** Binary inputs are programs and raw data files. */
    final case class Binary(prefix: String) extends Source {
      override def matches(key: String): Boolean = key == prefix
    }
  }
}
