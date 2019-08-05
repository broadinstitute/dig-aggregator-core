package org.broadinstitute.dig.aggregator.pipeline.intake

import cats.effect.IO

import java.util.UUID

import org.broadinstitute.dig.aggregator.core.{Processor, Run}
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.pipeline.Pipeline

/** All processors for the meta-analysis pipeline.
  */
object IntakePipeline extends Pipeline {
  import Processor.Name
  import Processor.register

  /** The IntakePipeline doesn't really exist. Instead, it's just a place-holder for
    * the HDFS:<topic> processors that are used as dependencies for the other
    * pipelines.
    *
    * The actual Intake code is in https://github.com/broadinstitute/dig-aggregator-intake
    * which is a Python project.
    */
  private def createDummyProcessor(processorName: Name, config: BaseConfig): Processor = {
    new Processor(processorName, config) {

      /** No dependencies. */
      override val dependencies: Seq[Name] = Seq.empty

      /** No resources to upload. */
      override val resources: Seq[String] = Seq.empty

      /** There is no work for this processor... ever. */
      override def getWork(opts: Processor.Opts): IO[Map[String, Set[UUID]]] = IO(Map.empty)

      /** There are never any outputs generated by this processor. */
      override def getOutputs(input: Run.Result): Processor.OutputList = Processor.NoOutputs

      /** Do nothing, because this will never be called. */
      override def processOutputs(outputs: Seq[String]): IO[_] = IO.unit

      /** Never does anything. */
      override def run(opts: Processor.Opts): IO[Unit] = IO.unit
    }
  }

  /** The various intake processors. */
  val variants: Name       = register("HDFS:variants", createDummyProcessor)
  val chromatinState: Name = register("HDFS:chromatin_state", createDummyProcessor)
}
