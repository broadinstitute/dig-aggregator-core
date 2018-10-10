package org.broadinstitute.dig.aggregator.core

import cats.effect._

import org.broadinstitute.dig.aggregator.core.processors._

import org.scalatest.FunSuite

object TestProcessor {
  import Processor.Name

  /**
   * Creates a new instance of a dummy processor.
   */
  def makeProcessor(processorName: Name)(c: config.BaseConfig): Processor = {
    new Processor {
      val name: Name = processorName

      /**
       * Does nothing, just here for the trait.
       */
      def run(flags: Processor.Flags): IO[Unit] = {
        IO(println(name.toString))
      }
    }
  }

  // create some test processors
  val a: Name = new Name("A", makeProcessor(a))
  val b: Name = new Name("B", makeProcessor(b))
  val c: Name = new Name("C", makeProcessor(c))
  val d: Name = new Name("D", makeProcessor(d))
  val e: Name = new Name("E", makeProcessor(e))
}
