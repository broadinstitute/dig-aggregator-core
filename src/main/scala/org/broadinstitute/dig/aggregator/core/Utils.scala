package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.syntax.all._

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/**
 * Utility functions.
 */
object Utils {
  import Implicits.timer

  /**
   * Attempt to run an IO operation. If it fails, wait a little bit and then
   * try again up to `retries` times. Use an exponential backoff, so it will
   * wait longer after each attempt.
   */
  def retry[A](ioa: IO[A], delay: FiniteDuration, retries: Int = 4): IO[A] = {
    ioa.handleErrorWith { error =>
      if (retries > 0) {
        IO.sleep(delay) *> retry(ioa, delay * 2, retries - 1)
      } else {
        IO.raiseError(error)
      }
    }
  }
}
