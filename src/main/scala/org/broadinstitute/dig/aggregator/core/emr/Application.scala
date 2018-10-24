package org.broadinstitute.dig.aggregator.core.emr

sealed abstract class ApplicationName(val name: String) 

object ApplicationName {
  case object Hadoop extends ApplicationName("Hadoop")
  case object Spark extends ApplicationName("Spark")
  case object Hive extends ApplicationName("Hive")
  case object Pig extends ApplicationName("Pig")
}
