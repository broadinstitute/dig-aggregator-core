package org.broadinstitute.dig.aggregator.core.config

/**
 * Sendgrid email configuration settings.
 */
final case class Sendgrid(
    key: String,
    from: String,
    emails: List[String]
)
