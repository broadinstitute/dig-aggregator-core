package org.broadinstitute.dig.aggregator.core.config

/** Private configuration settings required by pretty much everything.
  */
final case class BaseConfig(
    aws: AWSConfig,
    mysql: MySQLConfig,
    neo4j: Neo4jConfig,
)
