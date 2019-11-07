package org.broadinstitute.dig.aggregator.core.processors.loamstream

import org.broadinstitute.dig.aggregator.core.Processor
import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import java.net.URI
import _root_.loamstream.loam.LoamGraph
import _root_.loamstream.compiler.LoamEngine
import _root_.loamstream.compiler.LoamEngine
import loamstream.compiler.LoamProject
import loamstream.apps.LoamRunner
import loamstream.util.HeterogeneousMap
import java.nio.file.Path
import loamstream.util.Hit
import loamstream.util.Miss
import loamstream.conf.LoamConfig

abstract class LoamProcessor(
    override val name: Processor.Name, 
    config: BaseConfig,
    loamConfig: LoamConfig,
    loamRunner: LoamRunner,
    loams: Iterable[Path]) extends Processor(name, config) {
  
  protected def resourcesByName: Map[String, URI]
  
  protected def loamCode: LoamProject = {
    LoamEngine.scriptsFrom(loams.toSet).map(_.toSet) match {
      case Hit(scripts) => LoamProject(loamConfig, scripts)
      case m: Miss => sys.error(m.message)
    }
  }
  
  override def processOutputs(outputs: Seq[String]): IO[_] = {
    val processorParams = ProcessorContext(processor = this, resources = resourcesByName, outputs = outputs)
    
    invokeLoamStream(processorParams)
  }
  
  protected def invokeLoamStream(processorContext: ProcessorContext): IO[_] = IO {
    import LoamProcessor.processorParamsKey
    
    println(s"@@@@@@@@@@@ invoking LoamStream with context: $processorContext")
    
    loamRunner.run(loamCode, Seq(processorParamsKey ~> processorContext))
  }
}

object LoamProcessor {
  val processorParamsKey: HeterogeneousMap.Key[String, ProcessorContext] = {
    HeterogeneousMap.keyFor[ProcessorContext].of("LoamProcessor.processorParamsKey") 
  }
}
