package org.broadinstitute.dig.aggregator.core.processors.loamstream

import loamstream.loam.LoamScriptContext

object AggregatorSupport {
  def processorContext(implicit context: LoamScriptContext): ProcessorContext = {
    val properties = context.projectContext.propertiesMap
    
    val propertiesKey = LoamProcessor.processorParamsKey
    
    require(properties.get(propertiesKey).isDefined)
    
    properties(propertiesKey)
  }
}
