package org.broadinstitute.dig.aggregator.core

import cats.effect._
import scala.concurrent.ExecutionContext

object Implicits {

  /** Needed for IO.sleep. */
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  /** Needed for IO.parSequence. */
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
}
