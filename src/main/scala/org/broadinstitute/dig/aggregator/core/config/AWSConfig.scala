package org.broadinstitute.dig.aggregator.core.config

import org.broadinstitute.dig.aggregator.core.config.emr._

/** AWS configuration settings. */
final case class AWSConfig(
    s3: S3Config,
    emr: EmrConfig,
)

/** S3 configuration settings. */
final case class S3Config(
    bucket: String,
)
