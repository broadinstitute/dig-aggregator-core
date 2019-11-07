package org.broadinstitute.dig.aggregator.core.processors.loamstream

import java.net.URI

import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.config.BaseConfig

import org.broadinstitute.dig.aws.AWS

import _root_.loamstream.apps.LoamRunner
import _root_.loamstream.loam.LoamGraph
import _root_.loamstream.compiler.LoamEngine
import _root_.loamstream.compiler.LoamProject

import org.broadinstitute.dig.aggregator.core.Run
import org.broadinstitute.dig.aggregator.pipeline.intake.IntakePipeline
import java.nio.file.Path
import loamstream.util.Hit
import loamstream.conf.LoamConfig
import loamstream.util.Miss



final class FreqAnalysisProcessor(
    override val name: Processor.Name, 
    config: BaseConfig,
    loamConfig: LoamConfig,
    loamRunner: LoamRunner,
    loams: Iterable[Path]) extends LoamProcessor(name, config, loamConfig, loamRunner, loams) {
  
  override protected def resourcesByName: Map[String, URI] = {
    Map("frequencyAnalysis.py" -> aws.uriOf("resources/pipeline/frequencyanalysis/frequencyAnalysis.py")) 
  }
  
  /** Source data to consume.
    */
  override val dependencies: Seq[Processor.Name] = Seq(IntakePipeline.variants)

  
  /** Determine the output(s) for each input.
    *
    * The inputs are the dataset names. The outputs are the phenotype represented
    * within that dataset.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    val pattern = raw"([^/]+)/(.*)".r

    input.output match {
      case pattern(_, phenotype) => Processor.Outputs(Seq(phenotype))
    }
  }
}
