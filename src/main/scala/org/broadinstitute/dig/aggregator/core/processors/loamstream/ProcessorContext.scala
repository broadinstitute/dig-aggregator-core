package org.broadinstitute.dig.aggregator.core.processors.loamstream

import java.net.URI
import org.broadinstitute.dig.aggregator.core.Processor

final case class ProcessorContext(processor: Processor, resources: Map[String, URI], outputs: Seq[String]) {
  def name: String = processor.name.toString
}
