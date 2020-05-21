package org.broadinstitute.dig.aggregator.pipeline.gregor

import org.broadinstitute.dig.aggregator.core.{Stage, Run}
import org.broadinstitute.dig.aws.JobStep

class SortRegionsStage extends Stage {

  /** Dependencies.
    */
  override val dependencies: Seq[Run.Input.Source] = Seq(
    Run.Input.Source.Dataset("annotated_regions/"),
  )

  /** All input datasets map to a single output.
    */
  override def getOutputs(input: Run.Input): Stage.Outputs = {
    Stage.Outputs.Set("regions")
  }

  /** Take any new datasets and convert them from JSON-list to BED file
    * format with all the appropriate headers and fields. All the datasets
    * are processed together by the Spark job, so what's in the results
    * input doesn't matter.
    */
  override def getJob(output: String): Seq[JobStep] = {
    val script = resourceURI("pipeline/gregor/sortRegions.py")

    // run all the jobs then update the database
    Seq(JobStep.PySpark(script))
  }
}
