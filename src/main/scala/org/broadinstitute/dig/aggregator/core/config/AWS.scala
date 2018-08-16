package org.broadinstitute.dig.aggregator.core.config

/**
 * AWS configuration settings.
 */
final case class AWS(
    key: String,
    secret: String,
    region: String,
    emr: EMR,
    s3: S3
)

/**
 * Optional AWS EMR settings.
 */
final case class EMR(cluster: String)

/**
 * Optional AWS S3 settings.
 */
final case class S3(bucket: String)
