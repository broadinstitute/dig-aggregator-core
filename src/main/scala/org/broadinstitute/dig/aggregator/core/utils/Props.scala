package org.broadinstitute.dig.aggregator.core.utils

import java.util.Properties

/**
 * @author clint
 * Jul 22, 2018
 */
object Props {

  /**
   * Utility method for making java.util.Properties inline.
   */
  def apply(tuples: (String, String)*): Properties = {
    val props = new Properties

    tuples.foreach { case (key, value) => props.put(key, value) }

    props
  }
}
