package org.broadinstitute.dig.aggregator.core.config

import cats._
import cats.effect._
import cats.implicits._

import com.sendgrid._

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Sendgrid email configuration settings.
 */
final case class SendgridConfig(key: String, from: String, emails: List[String]) {

  /**
   * Send an email to to everyone in the configuration list.
   */
  def send(subject: String, message: String): IO[Unit] = {
    val client    = new SendGrid(key)
    val fromEmail = new Email(from)
    val content   = new Content("text/plain", message)

    val ios = for (to <- emails) yield {
      val mail = new Mail(fromEmail, subject, new Email(to), content)
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
