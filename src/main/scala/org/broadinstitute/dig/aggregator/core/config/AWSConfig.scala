package org.broadinstitute.dig.aggregator.core.config

import java.net.URI

import org.broadinstitute.dig.aggregator.core.config.emr._

/**
 * AWS configuration settings.
 */
final case class AWSConfig(
    key: String,
    secret: String,
    region: String,
    bucket: String,
    emr: EmrConfig,
)
