package org.broadinstitute.dig.aggregator.core

import cats.effect._
import cats.syntax.all._

import fs2._

import scala.concurrent.duration._

/** Utility functions. */
object Utils {
  import Implicits.timer

  /** Helper to allow performing an IO operation, but ignore the results. */
  val ignoreIO: IO[_] => IO[Unit] = _.map(scala.Function.const(()))

  /** Attempt to run an IO operation. If it fails, wait a little bit and then
    * try again up to `retries` times.
    */
  def retry[A](ioa: IO[A], retries: Int = 10): IO[A] = {
    ioa.handleErrorWith { error =>
      if (retries > 0) {
        IO.sleep(2.seconds) *> retry(ioa, retries - 1)
      } else {
        IO.raiseError(error)
      }
    }
  }

  /** Given a sequence of IO tasks, run them in parallel, but limit the maximum
    * concurrency so too many clusters aren't created at once.
    *
    * Optionally, apply a mapping function for each.
    */
  def waitForTasks[A, R](tasks: Seq[IO[A]], limit: Int = 5)(mapEach: IO[A] => IO[R] = ignoreIO): IO[Unit] = {
    import Implicits.contextShift

    Stream
      .emits(tasks)
      .covary[IO]
      .mapAsyncUnordered(limit)(mapEach)
      .compile
      .toList
      .as(())
  }
}
