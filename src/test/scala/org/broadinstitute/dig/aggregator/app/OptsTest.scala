package org.broadinstitute.dig.aggregator.app

import org.scalatest.FunSuite
import org.rogach.scallop.exceptions.ScallopException

/**
 * @author clint
 * Jul 24, 2018
 */
final class OptsTest extends FunSuite {

  private def opts(commandLine: String): Opts = {
    new Opts(commandLine.split("\\s+"))
  }

  private val confFile = "src/test/resources/config.json"

  test("flags") {
    {
      val o = opts(s"--version --config $confFile")

      o.verify

      assert(o.version() === true)
      assert(o.reprocess() === false)
    }

    {
      val o = opts(s"--reprocess --config $confFile foo")

      o.verify

      assert(o.version() === false)
      assert(o.reprocess() === true)
      assert(o.processor() === "foo")
    }
  }
}
