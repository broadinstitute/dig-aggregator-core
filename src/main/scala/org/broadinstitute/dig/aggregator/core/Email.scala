package org.broadinstitute.dig.aggregator.core

import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core.config.SendgridConfig

/** Helper class for sending emails to everyone on a list. */
class Email(config: SendgridConfig) {
  import com.sendgrid

  /** Send an email to everyone in the configuration. */
  def send(subject: String, body: String): IO[Unit] = {
    val client    = new sendgrid.SendGrid(config.key)
    val fromEmail = new sendgrid.Email(config.from)
    val content   = new sendgrid.Content("text/plain", body)

    val ios = for (to <- config.emails) yield {
      val mail = new sendgrid.Mail(fromEmail, subject, new sendgrid.Email(to), content)
      val req  = new sendgrid.Request()

      IO {
        req.setMethod(sendgrid.Method.POST)
        req.setEndpoint("mail/send")
        req.setBody(mail.build)

        // send the email
        client.api(req)
      }
    }

    // send each of the emails in parallel
    ios.sequence >> IO.unit
  }
}
