package org.broadinstitute.dig.aggregator.core

import org.scalatest.FunSuite
import org.rogach.scallop.exceptions.ScallopException

/**
 * @author clint
 * Jul 24, 2018
 */
final class OptsTest extends FunSuite {

  private def opts(commandLine: String): Opts[Config] = {
    new Opts(commandLine.split("\\s+"))
  }

  private val confFile = "src/test/resources/config.json"

  test("flags") {
    {
      val o = opts(s"--version --config $confFile")

      assert(o.version() === true)
      assert(o.continue() === true)
      assert(o.reset() === false)
    }

    {
      val o = opts(s"--reset --config $confFile")

      assert(o.version() === false)
      assert(o.continue() === false)
      assert(o.reset() === true)
    }
  }
}
