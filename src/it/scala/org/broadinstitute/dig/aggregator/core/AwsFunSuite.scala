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
  protected def aws: AWS[_]
  
  def testWithPseudoDir(name: String)(body: String => Any): Unit = {
    test(name) {
      val mungedName = name.filter(_ != AWS.pathSep)
      
      val pseudoDirKey = s"integrationTests${AWS.pathSep}${mungedName}"
      
      val nukeTestDir = for {
        keys <- aws.ls(s"${pseudoDirKey}${AWS.pathSep}")
        _ <- aws.rm(keys)
      } yield ()
      
      nukeTestDir.unsafeRunSync()
      
      body(pseudoDirKey)
    }
  }
  
  def testWithPseudoDirIO[A](name: String)(body: String => IO[A]): Unit = {
    testWithPseudoDir(name)(body(_).unsafeRunSync())
  }
}
