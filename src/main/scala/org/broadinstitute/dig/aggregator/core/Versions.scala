package org.broadinstitute.dig.aggregator.core

import scala.util.Try
import java.util.Properties
import java.time.Instant
import java.io.Reader
import java.io.InputStreamReader
import java.io.InputStream
import scala.util.Failure
import scala.util.Success

/**
 * @author clint
 * Aug 1, 2018
 * 
 * Based on the Versions class from LoamStream, created Oct 28, 2016.
 */
final case class Versions(
    name: String, 
    version: String, 
    branch: String, 
    lastCommit: Option[String], 
    anyUncommittedChanges: Boolean,
    describedVersion: Option[String],
    buildDate: Instant) {
  
  override def toString: String = {
    val isDirtyPhrase = if(anyUncommittedChanges) " (PLUS uncommitted changes!) " else " "
    
    val branchPhrase = s"branch: $branch"
      
    val describedVersionPhrase = describedVersion.getOrElse("UNKNOWN")
    
    val commitPhrase = s"commit: ${lastCommit.getOrElse("UNKNOWN")}"
    
    val buildDatePhrase = s"built on: $buildDate"
      
    s"$name $version ($describedVersionPhrase) $branchPhrase $commitPhrase$isDirtyPhrase$buildDatePhrase"
  }
}

object Versions {
  object DefaultPropsFileNames {
    val forAggregatorCore: String = "dig-aggregator-core-versionInfo.properties"
  }
  
  private[core] def propsFrom(propsFile: String): Try[Properties] = {
    val propStreamOption = Option(getClass.getClassLoader.getResourceAsStream(propsFile))
    
    val propStreamAttempt = toTry(propStreamOption)(s"Couldn't find '$propsFile' on the classpath")
    
    for {
      propStream <- propStreamAttempt
      reader <- Try(new InputStreamReader(propStream))
      props <- toProps(reader)
    } yield props
  }
  
  def load(versionsPropsFile: String): Try[Versions] = {
    for {
      props <- propsFrom(versionsPropsFile)
      versions <- loadFrom(props)
    } yield {
      versions
    }
  }
  
  private[core] def loadFrom(reader: Reader): Try[Versions] = toProps(reader).flatMap(loadFrom)
  
  private[core] def loadFrom(props: Properties): Try[Versions] = {
    import Implicits._
    
    for {
      name <- props.tryGetProperty("name")
      version <- props.tryGetProperty("version")
      branch <- props.tryGetProperty("branch")
      lastCommit = props.tryGetProperty("lastCommit").toOption
      anyUncommittedChanges <- props.tryGetProperty("uncommittedChanges").map(_.toBoolean)
      describedVersion = props.tryGetProperty("describedVersion").toOption
      buildDate <- props.tryGetProperty("buildDate").map(Instant.parse)
    } yield {
      Versions(name, version, branch, lastCommit, anyUncommittedChanges, describedVersion, buildDate)
    }
  }
  
  object Implicits {
    final implicit class PropertiesOps(val props: Properties) extends AnyVal {
      def tryGetProperty(key: String): Try[String] = {
        toTry(Option(props.getProperty(key)).map(_.trim).filter(_.nonEmpty)) {
          import scala.collection.JavaConverters._
          
          val sortedPropKvPairs = props.asScala.toSeq.sortBy { case (k, _) => k }
          
          s"property key '$key' not found in $sortedPropKvPairs"
        }
      }
    }
  }
  
  private def toTry[A](o: Option[A])(messageIfNone: => String): Try[A] = o match {
    case Some(a) => Success(a)
    case None => Failure(new Exception(messageIfNone))
  }
  
  private[core] def toProps(reader: Reader): Try[Properties] = Try {
    try {
      val props = new Properties
          
      props.load(reader)
          
      props
    } finally {
      reader.close()
    }
  }
}
