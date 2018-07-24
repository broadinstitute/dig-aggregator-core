package org.broadinstitute.dig.aggregator.core

import java.util.Properties

object Props {
  def apply(tuples: (String, String)*): Properties = {
    val props = new Properties
    
    tuples.foreach { case (key, value) => props.put(key, value) }
    
    props
  }
}
