package org.broadinstitute.dig.aggregator.core

import cats.effect._
import cats.syntax.all._

import fs2._

import scala.concurrent.duration._

/** Utility functions. */
object Utils {
  import Implicits.timer

  /** Attempt to run an IO operation. If it fails, wait a little
    * bit and then try again up to `retries` times. Applies an
    * exponential backoff each attempt.
    */
  def retry[A](ioa: IO[A], delay: FiniteDuration = 30.seconds, retries: Int = 10): IO[A] = {
    ioa.handleErrorWith { error =>
      if (retries > 0) {
        IO.sleep(delay) *> retry(ioa, delay * 2, retries - 1)
      } else {
        IO.raiseError(error)
      }
    }
  }
}
