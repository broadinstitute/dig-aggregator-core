package org.broadinstitute.dig.aggregator.core.config

import java.util.Base64

import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest
import com.amazonaws.services.secretsmanager.{AWSSecretsManager, AWSSecretsManagerClientBuilder}

import org.json4s.jackson.Serialization.read
import org.json4s.{DefaultFormats, Formats}

import scala.util.Try

object Secrets {

  /** Secrets manager.
    */
  private val client: AWSSecretsManager = AWSSecretsManagerClientBuilder.defaultClient

  /** Fetch a secret as JSON and decode it.
    */
  def get[A](secretId: String)(implicit m: Manifest[A]): Try[A] = {
    implicit val formats: Formats = DefaultFormats

    // create the request
    val req = new GetSecretValueRequest().withSecretId(secretId)

    // fetch the secret and attempt to parse it
    Try(client.getSecretValue(req)).map { resp =>
      val secret = if (resp.getSecretString != null) {
        resp.getSecretString
      } else {
        new String(Base64.getDecoder.decode(resp.getSecretBinary).array)
      }

      read[A](secret)
    }
  }
}
