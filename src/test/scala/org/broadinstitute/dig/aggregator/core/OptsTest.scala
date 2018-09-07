package org.broadinstitute.dig.aggregator.core

import org.scalatest.FunSuite
import org.rogach.scallop.exceptions.ScallopException

/**
 * @author clint
 * Jul 24, 2018
 */
final class OptsTest extends FunSuite {

  private val appName = "OptsTest"
  
  private def opts(commandLine: String): Opts = {
    new Opts(appName, commandLine.split("\\s+"))
  }

  private val confFile = "src/test/resources/config.json"

  test("flags") {
    {
      val o = opts(s"--version --config $confFile")

      assert(o.version() === true)
      assert(o.reprocess() === false)
      assert(o.reprocessAll() === false)
    }

    {
      val o = opts(s"--reprocess --config $confFile")

      assert(o.version() === false)
      assert(o.reprocess() === true)
      assert(o.reprocessAll() === false)
    }
    
    {
      val o = opts(s"--reprocess --reprocess-all --config $confFile")

      assert(o.version() === false)
      assert(o.reprocess() === true)
      assert(o.reprocessAll() === true)
    }
    
    //--reprocess-all can't be alone
    intercept[Exception] {
      opts(s"--reprocess-all --config $confFile")
    }
  }
  
  test("ignoreProcessedBy") {
    val appNameOpt = Option(appName)
    
    assert(opts(s"--version --config $confFile").ignoreProcessedBy === appNameOpt)
    assert(opts(s"--reprocess --config $confFile").ignoreProcessedBy === appNameOpt)
    
    assert(opts(s"--reprocess --reprocess-all --config $confFile").ignoreProcessedBy === None)
  }
}
