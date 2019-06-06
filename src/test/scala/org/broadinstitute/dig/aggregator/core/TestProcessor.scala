package org.broadinstitute.dig.aggregator.core

import cats.effect._

import org.broadinstitute.dig.aggregator.core.processors._

import org.scalatest.FunSuite

object TestProcessor {
  import Processor.{Name, register}

  /** Creates a new instance of a dummy processor.
    */
  def makeProcessor(processorName: Name, c: config.BaseConfig): Processor[_ <: Run.Input] = {
    new Processor[Dataset](processorName) {

      /** No resources to upload.
        */
      override val resources: Seq[String] = Seq.empty

      /** There is no work for this processor.
        */
      override def getWork(opts: Processor.Opts): IO[Seq[Dataset]] = IO(Seq.empty)

      /** There are no outputs generated.
        */
      override def getRunOutputs(work: Seq[Dataset]): Map[String, Seq[String]] = Map.empty

      /** Does nothing, just here for the trait.
        */
      override def run(opts: Processor.Opts): IO[Unit] = {
        IO(println(name.toString))
      }
    }
  }

  // create some test processors
  val a: Name = register("A", makeProcessor)
  val b: Name = register("B", makeProcessor)
  val c: Name = register("C", makeProcessor)
  val d: Name = register("D", makeProcessor)
  val e: Name = register("E", makeProcessor)
}
