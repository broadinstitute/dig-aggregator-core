package org.broadinstitute.dig.aggregator.core

import cats.effect._

import org.broadinstitute.dig.aggregator.core.processors._

import org.scalatest.FunSuite

object TestProcessor {
  import Processor.{Name, register}

  /**
   * Creates a new instance of a dummy processor.
   */
  def makeProcessor(processorName: Name, c: config.BaseConfig): Processor = {
    new Processor(processorName) {

      /**
       * There is no work for this processor.
       */
      override def getWork(reprocess: Boolean, only: Option[String]): IO[Seq[_]] = IO(Seq())

      /**
       * Does nothing, just here for the trait.
       */
      override def run(reprocess: Boolean, only: Option[String]): IO[Unit] = {
        IO(println(name.toString))
      }
    }
  }

  // create some test processors
  val a: Name = register("A", makeProcessor(_, _))
  val b: Name = register("B", makeProcessor(_, _))
  val c: Name = register("C", makeProcessor(_, _))
  val d: Name = register("D", makeProcessor(_, _))
  val e: Name = register("E", makeProcessor(_, _))
}
