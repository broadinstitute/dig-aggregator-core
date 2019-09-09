package org.broadinstitute.dig.aggregator.core

import org.scalatest.FunSuite
import org.broadinstitute.dig.aws.config.Secrets

/**
 * @author jmassung
 */
final class SecretsTest extends FunSuite {
  test("secrets") {
    val secret = Secrets.get[SecretsTest.TestSecret]("hello")

    assert(secret.isSuccess)
    assert(secret.get.hello == "world")
  }
}

object SecretsTest {
  final case class TestSecret(hello: String)
}
