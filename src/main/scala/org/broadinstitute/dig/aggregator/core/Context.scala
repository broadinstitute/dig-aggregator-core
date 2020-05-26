package org.broadinstitute.dig.aggregator.core

import org.broadinstitute.dig.aws.Emr
import org.broadinstitute.dig.aws.S3

/** Method execution context. */
class Context(val method: Method) {

  /** Override to provide a valid database connection. */
  lazy val db: Db = throw new NotImplementedError("No DB defined in context!")

  /** Override to provide a valid S3 bucket. */
  lazy val s3: S3.Bucket = throw new NotImplementedError("No S3 client in context!")

  /** Override to provide a valid EMR job runner. */
  lazy val emr: Emr.Runner = throw new NotImplementedError("No EMR client in context!")
}

/** Method execution context for testing. */
class TestContext(override val method: Method) extends Context(method) {
  override lazy val db: Db = new Db()
}
