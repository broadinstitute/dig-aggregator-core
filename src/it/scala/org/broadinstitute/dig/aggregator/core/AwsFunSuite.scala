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
  def testWithPseudoDir(name: String)(body: String => Any): Unit = {
    test(name) {
      val mungedName = name.filter(_ != AWS.pathSep)
      
      val now = Instant.now
      
      import AwsFunSuite.dateFormatter
      
      val pseudoDirKey = s"integrationTests/${mungedName}/${dateFormatter.format(now)}"
      
      body(pseudoDirKey)
    }
  }
  
  def testWithPseudoDirIO[A](name: String)(body: String => IO[A]): Unit = {
    testWithPseudoDir(name)(body(_).unsafeRunSync())
  }
}

object AwsFunSuite {
  private val dateFormatter: DateTimeFormatter = {
    DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss.SSS").withZone(ZoneId.systemDefault)
  }
}
