package org.broadinstitute.dig.aggregator.core

import cats.effect._
import cats.implicits._

import com.sendgrid._

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Sends email notifications to anyone in the configuration list.
 */
class Notifier(opts: Opts) {

  /**
   * Sendgrid client.
   */
  val client = new SendGrid(opts.config.sendgrid.key)

  /**
   * List of recipients to notify.
   */
  val to = opts.config.sendgrid.emails.map(new Email(_))

  /**
   * Send an email to to everyone in the configuration list.
   */
  def send(subject: String, message: String): IO[Unit] = {
    val fromEmail = new Email(opts.config.sendgrid.from)
    val content   = new Content("text/plain", message)

    val ios = to.map { email =>
      val mail = new Mail(fromEmail, subject, email, content)
      val req  = new Request()

      IO {
        req.setMethod(Method.POST)
        req.setEndpoint("mail/send")
        req.setBody(mail.build)

        // send the email
        client.api(req)
      }
    }

    // send each of the emails in parallel
    ios.toList.parSequence >> IO.unit
  }
}
