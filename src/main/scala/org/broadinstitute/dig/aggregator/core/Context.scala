package org.broadinstitute.dig.aggregator.core

import org.broadinstitute.dig.aws.Emr
import org.broadinstitute.dig.aws.S3
import org.broadinstitute.dig.aws.config.RdsConfig

/** Method execution context. */
abstract class Context(val method: Method, val config: Option[Config]) {

  /** Override to provide a valid database connection. */
  lazy val db: Db = throw new NotImplementedError("No DB defined in context!")

  /** Override to provide a valid database connection. */
  lazy val portal: RdsConfig = throw new NotImplementedError("No portal Config defined in context!")

  /** Override to provide a valid S3 input bucket. */
  lazy val s3: S3.Bucket = throw new NotImplementedError("No S3 input in context!")

  /** Override to provide a valid S3 output bucket. */
  lazy val s3Output: S3.Bucket = throw new NotImplementedError("No S3 output in context!")

  /** Override to provide a valid S3 bioindex bucket. */
  lazy val s3Bioindex: S3.Bucket = throw new NotImplementedError("No S3 bioindex in context!")

  /** Override to provide a valid EMR job runner. */
  lazy val emr: Emr.Runner = throw new NotImplementedError("No EMR client in context!")
}

/** Method execution context for testing. */
class TestContext(override val method: Method) extends Context(method, None) {
  override lazy val db: Db = new Db()
}
