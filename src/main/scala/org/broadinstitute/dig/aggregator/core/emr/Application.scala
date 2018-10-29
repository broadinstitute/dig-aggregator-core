package org.broadinstitute.dig.aggregator.core.emr

import com.amazonaws.services.elasticmapreduce.model.Application

sealed abstract class ApplicationName(val name: String) {
  final def toApplication: Application = (new Application).withName(name)
}

object ApplicationName {
  case object Hadoop extends ApplicationName("Hadoop")
  case object Spark extends ApplicationName("Spark")
  case object Hive extends ApplicationName("Hive")
  case object Pig extends ApplicationName("Pig")
}
