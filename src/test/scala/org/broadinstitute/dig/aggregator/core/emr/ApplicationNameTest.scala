package org.broadinstitute.dig.aggregator.core.emr

import org.scalatest.FunSuite

/**
 * @author clint
 * Oct 30, 2018
 */
final class ApplicationNameTest extends FunSuite {
  test("names") {
    assert(ApplicationName.Hadoop.name === "Hadoop")
    assert(ApplicationName.Spark.name === "Spark")
    assert(ApplicationName.Hive.name === "Hive")
    assert(ApplicationName.Pig.name === "Pig")
  }
  
  test("toApplication") {
    def doTest(appName: ApplicationName, expectedName: String): Unit = {
      val app = appName.toApplication
      
      assert(app.getName === expectedName)
      assert(app.getAdditionalInfo.isEmpty)
      assert(app.getArgs.isEmpty)
      assert(app.getVersion === null)
    }
    
    doTest(ApplicationName.Hadoop, "Hadoop")
    doTest(ApplicationName.Hive, "Hive")
    doTest(ApplicationName.Spark, "Spark")
    doTest(ApplicationName.Pig, "Pig")
  }
}
