package org.broadinstitute.dig.aggregator.core

import org.broadinstitute.dig.aws.emr.Job

object TestMethod extends Method {

  // define a simple stage class
  class TestStage(implicit context: Context) extends Stage {
    val sourceA: Input.Source = Input.Source("a/*/", "*", None)
    val sourceB: Input.Source = Input.Source("b/*/", "*", None)

    /** No dependencies to upload. */
    override val sources: Seq[Input.Source] = Seq.empty

    /** Don't actually do any work. */
    override def make(output: String): Job = new Job(Seq.empty)

    /** Match some fake inputs to dummy outputs. */
    override val rules: PartialFunction[Input, Outputs] = {
      case sourceA(a, _) => Outputs.Named(a)
      case sourceB(b, _) => Outputs.Named(b)
      case _             => Outputs.All
    }
  }

  // create some test stages
  override def initStages(implicit context: Context): Unit = {
    addStage(new TestStage)
  }
}
