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
  
  test("--from-beginning and --continue are mutually exclusive") {
    intercept[ScallopException] {
      opts(s"--from-beginning --continue --config $confFile")
    }
    
    intercept[ScallopException] {
      opts("--continue --from-beginning --config $confFile")
    }
    
    //one at a time is fine
    
    opts(s"--from-beginning --config $confFile")
    
    opts(s"--continue --config $confFile")
  }
  
  test("position") {
    assert(opts(s"--config $confFile").position == State.End)
    assert(opts(s"--from-beginning --config $confFile").position == State.Beginning)
    assert(opts(s"--continue --config $confFile").position == State.Continue)
  }
}
