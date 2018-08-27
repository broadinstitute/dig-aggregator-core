package org.broadinstitute.dig.aggregator.core

import org.scalatest.FunSuite
import java.time.format.DateTimeFormatter
import java.time.ZoneId
import java.time.Instant
import cats.effect.IO

/**
 * @author clint
 * Jul 27, 2018
 */
trait AwsFunSuite extends FunSuite {
  protected def aws: AWS
  
  def testWithPseudoDir(name: String)(body: String => Any): Unit = {
    test(name) {
      val mungedName = name.filter(_ != '/')
      
      val pseudoDirKey = s"integrationTests/${mungedName}"
      
      def nukeTestDir() = aws.rmdir(s"${pseudoDirKey}/").unsafeRunSync()
      
      nukeTestDir()
      
      body(pseudoDirKey)
      
      //Test dir will be deleted after successful runs, but will live until the next run
      //if there's a failure.
      nukeTestDir()
    }
  }
  
  def testWithPseudoDirIO[A](name: String)(body: String => IO[A]): Unit = {
    testWithPseudoDir(name)(body(_).unsafeRunSync())
  }
}
