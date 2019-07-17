package org.broadinstitute.dig.aggregator.core

import cats.effect._

import java.util.UUID

import org.scalatest.FunSuite

object TestProcessor {
  import Processor.{Name, register}

  /** Creates a new instance of a dummy processor.
    */
  def makeProcessor(processorName: Name, c: config.BaseConfig): Processor = {
    new Processor(processorName, c) {

      /** No dependencies to upload. */
      override val dependencies: Seq[Processor.Name] = Seq.empty

      /** There is no work for this processor. */
      override def getWork(opts: Processor.Opts): IO[Seq[Run.Result]] = IO(Seq.empty)

      /** There are no outputs generated. */
      override def getRunOutputs(work: Seq[Run.Result]): Map[String, Seq[UUID]] = Map.empty

      /** Nothing to do. */
      override def processResults(results: Seq[Run.Result]): IO[Unit] = IO.unit

      /** Does nothing, just here for the trait. */
      override def run(opts: Processor.Opts): IO[Unit] = IO.unit
    }
  }

  // create some test processors
  val a: Name = register("A", makeProcessor)
  val b: Name = register("B", makeProcessor)
  val c: Name = register("C", makeProcessor)
  val d: Name = register("D", makeProcessor)
  val e: Name = register("E", makeProcessor)
}
