package org.broadinstitute.dig.aggregator.core

/**
 * Provenance is a simple data class used for an Analysis node so that given
 * any result node in the database, the analysis that produced it can be
 * found online and inspected.
 */
final case class Provenance(source: String, branch: String, commit: String)

/**
 * Companion object for creating Provenance from version information.
 */
object Provenance {

  /**
   * Create a new Provenance from a Versions properties file.
   */
  def apply(v: Versions): Provenance = {
    require(v.remoteUrl.isDefined, s"Versions missing remote url: '$v'")
    require(v.lastCommit.isDefined, s"Versions missing last commit: '$v'")

    Provenance(v.remoteUrl.get, v.branch, v.lastCommit.get)
  }

  /**
   * Default constructor will load the version information in the JAR.
   */
  def apply(): Provenance = {
    val propsFileName   = Versions.DefaultPropsFileNames.forDownstreamApps
    val versionsAttempt = Versions.load(propsFileName)

    def failureThrowable = versionsAttempt.failed.get

    require(versionsAttempt.isSuccess, s"Couldn't load version info from '${propsFileName}': ${failureThrowable}")

    apply(versionsAttempt.get)
  }
}
