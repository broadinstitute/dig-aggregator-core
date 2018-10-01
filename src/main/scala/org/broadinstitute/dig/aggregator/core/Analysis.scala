package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.implicits._

import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.StatementResult

/**
 * The representation of a processor's analysis that has been uploaded to the
 * graph database.
 *
 * The name is something that must be unique per application (e.g. phenotype),
 * as it is used to uniquely identify it within the database.
 */
final case class Analysis(app: String,
                          name: String,
                          typ: Analysis.Type,
                          provenance: Analysis.Provenance) {

  /**
   * Create an analysis node in the graph. This returned the unique, internal
   * ID of the node created (or updated) so that any result nodes can link to
   * it explicitly.
   */
  def update(driver: Driver, name: String): IO[Int] = {
    val q = s"""|CREATE (n:Analysis {
                |  name: '$app/$name',
                |  type: '${typ.toString}',
                |  source: ${provenance.source},
                |  branch: ${provenance.branch},
                |  commit: ${provenance.commit},
                |  created: timestamp(),
                |})
                |
                |// return the ID of the node created
                |RETURN ID(n)
                |""".stripMargin

    // run the query, return the node ID
    IO {
      driver.session.run(q).single.get(0).asInt
    }
  }
}

/**
 * Companion object for querying analyses in the graph database.
 */
object Analysis {

  /**
   * Every analysis is given a type.
   */
  sealed trait Type

  /**
   * The various types of analysis.
   */
  final case object Computation extends Type
  final case object Experiment  extends Type

  /**
   * Provenance is a simple data class used for an Analysis node so that given
   * any result node in the database, the analysis that produced it can be
   * found online and inspected.
   */
  final case class Provenance(
      source: String,
      branch: String,
      commit: String
  )
  
  object Provenance {
    def apply(v: Versions): Provenance = {
      require(v.remoteUrl.isDefined, s"Versions missing remote url: '$v'")
      require(v.lastCommit.isDefined, s"Versions missing last commit: '$v'")
      
      Provenance(v.remoteUrl.get, v.branch, v.lastCommit.get)
    }
    
    /**
     * Default constructor will load the version information in the JAR.
     */
    def apply(): Provenance = {
      //Note that we want the version info for the downstream app that depends on us, not this lib.
      val propsFileName = Versions.DefaultPropsFileNames.forDownstreamApps

      val versionsAttempt = Versions.load(propsFileName)
      
      def failureThrowable = versionsAttempt.failed.get
      
      require(versionsAttempt.isSuccess, s"Couldn't load version info from '${propsFileName}': ${failureThrowable}")
      
      apply(versionsAttempt.get)
    }
  }

  /**
   * Detatch and delete all nodes produced by a given :Analysis node and -
   * finally - the :Analysis node itself.
   */
  def deleteAnalysis(driver: Driver, app: String, name: String): IO[Unit] = {
    val q = s"""|MATCH (n)-[:PRODUCED_BY]->(:Analysis {name: '$app/$name'})
                |WITH n
                |LIMIT 50000
                |DETACH DELETE n
                |""".stripMargin

    // run the query
    val io = IO {
      val counters = driver.session.run(q).consume.counters

      // return the total number of nodes+relationships deleted
      counters.nodesDeleted + counters.relationshipsDeleted
    }

    // perform the query, if > 0 were deleted, recurse
    io.flatMap { n =>
      if (n > 0) {
        deleteAnalysis(driver, app, name)
      } else {
        val delete = s"MATCH (n:Analysis {name: '$app/$name'}) DETACH DELETE n"

        for {
          _ <- IO(driver.session.run(q))
        } yield ()
      }
    }
  }
}
