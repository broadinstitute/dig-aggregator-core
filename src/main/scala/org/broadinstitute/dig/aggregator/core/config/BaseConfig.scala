package org.broadinstitute.dig.aggregator.core.config

import org.broadinstitute.dig.aws.config.AWSConfig

/** Private configuration settings required by pretty much everything.
  */
final case class BaseConfig(
    aws: AWSConfig,
    mysql: MySQLConfig,
)
