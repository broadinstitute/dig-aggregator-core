package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._

import org.broadinstitute.dig.aggregator.core.processors.Processor

/**
 * Provenance is a simple data class used for an Analysis node so that given
 * any result node in the database, the analysis that produced it can be
 * found online and inspected.
 */
final case class Provenance(source: String, branch: String, commit: String) {

  /**
   * Insert a new provenance row for a given run.
   */
  def insert(xa: Transactor[IO], runId: Long, app: Processor.Name): IO[Int] = {
    import Processor.NameMeta

    val q = sql"""|INSERT INTO `provenance`
                  |  ( `run`
                  |  , `app`
                  |  , `source`
                  |  , `branch`
                  |  , `commit`
                  |  )
                  |
                  |VALUES
                  |  ( $runId
                  |  , $app
                  |  , $source
                  |  , $branch
                  |  , $commit
                  |  )
                  |""".stripMargin.update

    q.run.transact(xa)
  }
}

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
  lazy val thisBuild: Provenance = {
    val propsFileName   = Versions.DefaultPropsFileNames.forDownstreamApps
    val versionsAttempt = Versions.load(propsFileName)

    // a def so it won't evaluate unless there is an actual issue
    def failureThrowable = versionsAttempt.failed.get

    // check that the versions file loaded
    require(versionsAttempt.isSuccess, s"Couldn't load version info from '${propsFileName}': ${failureThrowable}")

    // return it
    apply(versionsAttempt.get)
  }

  /**
   * Get the provenance for a particular processor run.
   */
  def ofRun(xa: Transactor[IO], run: Long, app: Processor.Name): IO[Seq[Provenance]] = {
    import Processor.NameMeta

    val q = sql"""|SELECT  `source`,
                  |        `branch`,
                  |        `commit`
                  |
                  |FROM    `provenance`
                  |
                  |WHERE   `run` = $run
                  |AND     `app` = $app
                  |""".stripMargin.query[Provenance].to[Seq]

    q.transact(xa)
  }
}
