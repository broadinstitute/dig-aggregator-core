package org.broadinstitute.dig.aggregator.core

import org.scalatest.funsuite.AnyFunSuite
import org.broadinstitute.dig.aws.config.Secrets

/**
  * @author jmassung
  */
final class SecretsTest extends AnyFunSuite {
  test("secrets") {
    val secret = Secrets.get[SecretsTest.TestSecret]("hello")

    assert(secret.isSuccess)
    assert(secret.get.hello == "world")
  }
}

object SecretsTest {
  final case class TestSecret(hello: String)
}
