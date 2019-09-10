package org.broadinstitute.dig.aggregator.core

import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aws.config.Secrets

/** Helper class for sending emails to everyone on a list. */
object Email {
  import com.sendgrid

  /** Helper for decoding JSON. */
  final case class APIKey(key: String)

  /** Only look it up when needed. */
  lazy val apiKey: APIKey = Secrets.get[APIKey]("sendgrid").get

  /** Send an email to everyone in the configuration. */
  def send(to: String, subject: String, body: String): IO[Unit] = {
    val client    = new sendgrid.SendGrid(apiKey.key)
    val fromEmail = new sendgrid.Email("do-not-reply@broadinstitute.org")
    val content   = new sendgrid.Content("text/plain", body)
    val mail      = new sendgrid.Mail(fromEmail, subject, new sendgrid.Email(to), content)
    val req       = new sendgrid.Request()

    val io = IO {
      req.setMethod(sendgrid.Method.POST)
      req.setEndpoint("mail/send")
      req.setBody(mail.build)

      // send the email
      client.api(req)
    }

    // send each of the emails
    io.as(())
  }
}
