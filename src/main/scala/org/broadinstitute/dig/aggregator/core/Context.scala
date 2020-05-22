package org.broadinstitute.dig.aggregator.core

import org.broadinstitute.dig.aws.AWS

/** Context holds global state for the running method to use. */
case class Context(aws: AWS, db: DbPool, method: Method)

/** Companion object for creating a context. */
object Context {
  import scala.util.DynamicVariable
  import scala.util.Try

  /** Private context. */
  private val _current = new DynamicVariable[Context](null)

  /** Get the current context. */
  def current: Context = _current.value

  /** Create a new context and execute a body of code with it. */
  def use[T](method: Method)(body: => T)(implicit opts: Opts): Try[T] = {
    for (secret <- opts.config.aws.rds.secret) yield {
      val db  = DbPool(secret, "test")
      val aws = new AWS(opts.config.aws)

      // create the new context and execute
      _current.withValue(Context(aws, db, method))(body)
    }
  }
}
