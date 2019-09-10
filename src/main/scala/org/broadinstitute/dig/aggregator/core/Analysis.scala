package org.broadinstitute.dig.aggregator.core

import cats.effect.IO
import cats.implicits._

import com.typesafe.scalalogging.LazyLogging

import org.broadinstitute.dig.aws.AWS
import org.broadinstitute.dig.aggregator.core.Utils.retry

import org.neo4j.driver.v1.StatementResult

/** The representation of a processor's analysis that has been uploaded to the
  * graph database.
  *
  * The name of an analysis is something that has to be unique across all
  * analyses uploaded to the database as it is used to identify result nodes
  * produced by it.
  */
final class Analysis(val name: String, val provenance: Provenance) extends LazyLogging {
  import Implicits.contextShift

  /** Given an S3 glob to a list of part files, call the uploadPart function for
    * each, allowing the CSV to be written to Neo4j.
    */
  def uploadParts(aws: AWS, graph: GraphDb, analysisId: Long, s3path: String, ext: String = ".csv")(
      uploadPart: (GraphDb, Long, String) => IO[StatementResult]
  ): IO[Unit] = {
    for {
      listing <- aws.ls(s3path)

      // only keep the files with the proper extension
      parts = listing.filter(_.toLowerCase.endsWith(ext.toLowerCase))

      // indicate how many parts are being uploaded
      _ <- IO(logger.info(s"Uploading ${parts.size} part files for analysis '$name'..."))

      // create an IO statement for each part and upload it
      uploads = for ((part, n) <- parts.zipWithIndex) yield {
        val upload = retry(uploadPart(graph, analysisId, aws.publicUrlOf(part)))

        for (result <- upload) yield {
          val counters = result.consume.counters
          val nodes    = counters.nodesCreated
          val edges    = counters.relationshipsCreated

          logger.debug(s"...$nodes nodes & $edges relationships created (${n + 1}/${parts.size})")
        }
      }

      // upload the part files
      _ <- uploads.toList.sequence
      _ <- IO(logger.debug(s"Upload complete of analysis '$name'"))
    } yield ()
  }

  /** Create an analysis node in the graph. This returned the unique, internal
    * ID of the node created (or updated) so that any result nodes can link to
    * it explicitly.
    */
  def create(graph: GraphDb): IO[Long] = {
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
      _ <- delete(graph)

      // create the new analysis node after having deleted the previous one
      _ <- IO(logger.info(s"Creating new analysis for '$name'"))
      r <- graph.run(q)
    } yield r.single.get(0).asLong
  }

  /** Detach and delete all nodes produced by a given :Analysis node. Does not
    * delete the analysis node as it assumes it is being updated.
    */
  def delete(graph: GraphDb): IO[Unit] = {
    val batchSize = 10000

    // first delete all the results
    val deleteResults =
      s"""|CALL apoc.periodic.iterate(
          |  "MATCH (:Analysis {name: '$name'})<-[:PRODUCED]->(n) RETURN n",
          |  "DETACH DELETE n",
          |  {batchSize: $batchSize, parallel: false} // <- prevent deadlocks!
          |)
          |""".stripMargin

    // then delete the analysis
    val deleteAnalysis =
      s"""|MATCH (n:Analysis {name: '$name'})
          |DETACH DELETE n
          |""".stripMargin

    for {
      _ <- IO(logger.info(s"Deleting existing results for analysis '$name'"))
      _ <- graph.run(deleteResults)
      _ <- graph.run(deleteAnalysis)
    } yield ()
  }
}
