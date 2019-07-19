package org.broadinstitute.dig.aggregator.core.config

import org.scalatest.FunSuite

/** Tests for org.broadinstitute.dig.aggregator.core.emr._
  */
final class SecretsTest extends FunSuite {

  test("hello world") {
    val secret = Secrets.get[SecretsTest.TestSecret]("hello")

    assert(secret.isSuccess)
    assert(secret.get.hello == "world")
  }
}

object SecretsTest {
  case class TestSecret(hello: String)
}
