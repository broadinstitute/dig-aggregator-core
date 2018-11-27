package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._
import cats.implicits._

import com.typesafe.scalalogging.LazyLogging

import org.broadinstitute.dig.aggregator.core.processors.Processor

import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.StatementResult

/**
 * The representation of a processor's analysis that has been uploaded to the
 * graph database.
 *
 * The name of an analysis is something that has to be unique across all
 * analyses uploaded to the database as it is used to identify result nodes
 * produced by it.
 */
final class Analysis(val name: String, val provenance: Provenance) extends LazyLogging {

  /**
   * Given a list of part files associated with this analysis and a function
   * that runs a Neo4j query to upload each part...
   *
   *  * ...delete any existing analysis and relationships;
   *  * ...create a new analysis node;
   *  * ...call the upload function for each part file;
   */
  def uploadParts(aws: AWS, graph: GraphDb, analysisId: Int, s3path: String)(
      uploadPart: (GraphDb, Int, String) => IO[StatementResult]): IO[Unit] = {
    for {
      listing <- aws.ls(s3path)

      // only keep the part files that are CSV files which can be loaded
      parts = listing.filter(_.toLowerCase.endsWith(".csv"))

      // indicate how many parts are being uploaded
      _ <- IO(logger.debug(s"Uploading ${parts.size} part files..."))

      // create an IO statement for each part and upload it
      uploads = for ((part, n) <- parts.zipWithIndex) yield {
        for (result <- uploadPart(graph, analysisId, aws.publicUrlOf(part))) yield {
          val counters = result.consume.counters
          val nodes    = counters.nodesCreated
          val edges    = counters.relationshipsCreated

          logger.debug(s"...$nodes nodes & $edges relationships created (${n + 1}/${parts.size})")
        }
      }

      // upload each part serially
      _ <- uploads.toList.sequence
      _ <- IO(logger.debug("Upload complete"))
    } yield ()
  }

  /**
   * Create an analysis node in the graph. This returned the unique, internal
   * ID of the node created (or updated) so that any result nodes can link to
   * it explicitly.
   */
  def create(graph: GraphDb): IO[Int] = {
    val q = s"""|CREATE (n:Analysis {
                |  name: '$name',
                |  source: '${provenance.source}',
                |  branch: '${provenance.branch}',
                |  commit: '${provenance.commit}',
                |  created: timestamp()
                |})
                |
                |// return the ID of the node created
                |RETURN ID(n)
                |""".stripMargin

    // run the query, return the node ID
    for {
      _ <- IO(logger.debug(s"Deleting existing analysis for '$name'"))
      _ <- delete(graph)
      _ <- IO(logger.debug(s"Creating new analysis for '$name'"))
      r <- graph.run(q)
    } yield r.single.get(0).asInt
  }

  /**
   * Detatch and delete all nodes produced by a given :Analysis node. Does not
   * delete the analysis node as it assumes it is being updated.
   */
  def delete(graph: GraphDb): IO[Unit] = {

    // tail-recursive accumulator helper method to count total deletions
    def deleteResults(total: Int): IO[Int] = {
      val q = s"""|MATCH (n)-[:PRODUCED_BY]->(:Analysis {name: '$name'})
                  |WITH n
                  |LIMIT 50000
                  |DETACH DELETE n
                  |""".stripMargin

      // run the query
      val io = for (result <- graph.run(q)) yield {
        val counters = result.consume.counters
        val nodes    = counters.nodesDeleted
        val edges    = counters.relationshipsDeleted

        // return the total number of nodes+relationships deleted
        (nodes, edges)
      }

      io.flatMap {
        case (0, 0)         => IO(total)
        case (nodes, edges) => deleteResults(total + nodes + edges)
      }
    }

    /*
     * First delete all the result nodes produced by this analysis and all
     * the relationships coming from them. Then, delete the analysis node.
     */

    for {
      totalDeleted <- deleteResults(0)

      // delete the actual analysis node
      q = s"""|MATCH (n:Analysis {name: '$name'})
              |DETACH DELETE n
              |""".stripMargin

      // delete the actual node
      _ <- graph.run(q)

      // how how many result nodes and relationships were delete
      _ <- IO(logger.debug(s"Deleted $totalDeleted nodes and relationships"))
    } yield ()
  }
}
