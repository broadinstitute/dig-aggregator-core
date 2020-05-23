package org.broadinstitute.dig.aggregator.core

import org.broadinstitute.dig.aws.JobStep

object TestMethod extends Method {

  // define a simple stage
  object TestStage extends Stage {

    /** No dependencies to upload. */
    override val dependencies: Seq[Input.Source] = Seq.empty

    /** Don't actually do any work. */
    override def getJob(output: String): Seq[JobStep] = Seq.empty

    /** Match some fake inputs to dummy outputs. */
    override def getOutputs(input: Input): Outputs = {
      val testA = Glob("a/*/...")
      val testB = Glob("b/*/...")

      input.key match {
        case testA(a) => Outputs.Named(a)
        case testB(b) => Outputs.Named(b)
        case _        => Outputs.All
      }
    }
  }

  // create some test stages
  override def initStages(): Unit = {
    addStage(TestStage)
  }

  // automatically add stages for testing later
  initStages()
}
