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
final case class Analysis(app: String, name: String, typ: Analysis.Type, provenance: Provenance) {

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
                |  created: timestamp()
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
