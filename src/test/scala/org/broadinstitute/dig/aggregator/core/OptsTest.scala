package org.broadinstitute.dig.aggregator.core

import org.scalatest.FunSuite
import org.rogach.scallop.exceptions.ScallopException

/**
 * @author clint
 * Jul 24, 2018
 */
final class OptsTest extends FunSuite {
  
  private def opts(commandLine: String): Opts[Config] = new Opts(commandLine.split("\\s+"))
  
  test("--from-beginning and --continue are mutually exclusive") {
    intercept[ScallopException] {
      opts("--from-beginning --continue")
    }
    
    intercept[ScallopException] {
      opts("--continue --from-beginning")
    }
    
    //one at a time is fine
    
    opts("--from-beginning")
    
    opts("--continue")
  }
  
  test("position") {
    assert(opts(" ").position == State.End)
    assert(opts("--from-beginning").position == State.Beginning)
    assert(opts("--continue").position == State.Continue)
  }
}
