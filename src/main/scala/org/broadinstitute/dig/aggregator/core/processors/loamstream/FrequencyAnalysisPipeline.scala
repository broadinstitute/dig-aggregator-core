package org.broadinstitute.dig.aggregator.core.processors.loamstream

import org.broadinstitute.dig.aws.emr.Cluster
import org.broadinstitute.dig.aws.emr.InstanceType
import org.broadinstitute.dig.aws.emr.ApplicationConfig
import org.broadinstitute.dig.aws.emr.ClassificationProperties
import org.broadinstitute.dig.aws.JobStep


import _root_.loamstream.aws.AwsJobDesc
import _root_.loamstream.loam.LoamGraph
import _root_.loamstream.loam.LoamScriptContext
import _root_.loamstream.loam.LoamSyntax


object FrequencyAnalysisPipeline {
  def makeGraph(implicit context: LoamScriptContext): LoamGraph = {
    import LoamSyntax._
    import org.broadinstitute.dig.aggregator.core.processors.loamstream._
    val processorContext: ProcessorContext = AggregatorSupport.processorContext
    
    val cluster = Cluster(
        name = processorContext.name.toString,
        instances = 4,
        masterInstanceType = InstanceType.m5_2xlarge,
        slaveInstanceType = InstanceType.m5_2xlarge,
        configurations = Seq(
          ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3)
        )
      )
  
    val script: URI = processorContext.resources("frequencyAnalysis.py")
  
    // create the jobs to process each phenotype in parallel
    val jobs = processorContext.outputs.map { phenotype =>
      Seq(JobStep.PySpark(script, phenotype))
    }
    
    awsWith(cluster) {
      awsClusterJobs(jobs: _*)
    }
    
    context.projectContext.graph
  }
}
