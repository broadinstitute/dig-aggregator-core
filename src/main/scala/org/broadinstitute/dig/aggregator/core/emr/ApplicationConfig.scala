package org.broadinstitute.dig.aggregator.core.emr

import com.amazonaws.services.elasticmapreduce.model.Configuration

import java.net.URI

import scala.collection.JavaConverters._

/**
 * Each application can have various configuration settings assigned to it.
 *
 * See: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html
 */
sealed case class ApplicationConfig(classification: String, properties: (String, String)*) {

  /** Create a new App with additional configuration properties. */
  def withProperties(props: (String, String)*): ApplicationConfig = {
    new ApplicationConfig(classification, (properties ++ props): _*)
  }

  /** Create the EMR Configuration for this application. */
  def configuration: Configuration =
    new Configuration()
      .withClassification(classification)
      .withProperties(properties.toMap.asJava)
}

/**
 * Companion object containing some typical configurations.
 */
object ApplicationConfig {

  /** Some common configurations that can be extended. */
  val sparkDefaults = new ApplicationConfig("spark-defaults")
}
