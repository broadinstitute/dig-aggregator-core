package org.broadinstitute.dig.aggregator.pipeline.gregor

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aws.JobStep

class SortRegionsStage(implicit context: Context) extends Stage {
  val annotatedRegions: Input.Source = Input.Source.Dataset("annotated_regions/")

  /** Source inputs. */
  override val sources: Seq[Input.Source] = Seq(annotatedRegions)

  /** Map inputs to their outputs. */
  override val rules: PartialFunction[Input, Outputs] = {
    case annotatedRegions() => Outputs.Named("regions")
  }

  /** Take any new datasets and convert them from JSON-list to BED file
    * format with all the appropriate headers and fields. All the datasets
    * are processed together by the Spark job, so what's in the results
    * input doesn't matter.
    */
  override def make(output: String): Seq[JobStep] = {
    val script = resourceUri("pipeline/gregor/sortRegions.py")

    // run all the jobs then update the database
    Seq(JobStep.PySpark(script))
  }
}
