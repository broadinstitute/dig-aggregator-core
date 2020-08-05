package org.broadinstitute.dig.aggregator.core

import java.io.InputStreamReader
import java.util.Properties

import scala.util.Try

/** Provenance is a simple data class used for an Analysis node so that given
  * any result node in the database, the analysis that produced it can be
  * found online and inspected.
  */
final case class Provenance(source: Option[String], branch: Option[String], commit: Option[String])

/** Companion object for creating Provenance from version information. */
object Provenance {

  /** Load provenance data from a properties resource. */
  def fromResource(resource: String): Option[Provenance] = {
    for {
      stream <- Option(getClass.getClassLoader.getResourceAsStream(resource))

      // create a stream reader and properties object
      reader = new InputStreamReader(stream)
      props  = new Properties

      // attempt to parse the properties file
      _ <- Try(props.load(reader)).toOption

      // extract the provenance properties
      source = Option(props.getProperty("remoteUrl"))
      branch = Option(props.getProperty("branch"))
      commit = Option(props.getProperty("commit"))
    } yield Provenance(source, branch, commit)
  }
}
