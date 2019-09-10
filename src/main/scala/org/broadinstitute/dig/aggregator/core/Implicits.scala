package org.broadinstitute.dig.aggregator.core

import scala.concurrent.ExecutionContext

import cats.effect.ContextShift
import cats.effect.IO
import cats.effect.Timer


object Implicits {

  /** Needed for IO.sleep. */
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  /** Needed for IO.parSequence. */
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
}
