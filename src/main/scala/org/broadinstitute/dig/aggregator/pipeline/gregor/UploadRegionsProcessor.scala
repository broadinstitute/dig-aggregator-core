package org.broadinstitute.dig.aggregator.pipeline.gregor

import java.net.URL
import java.net.URLDecoder

import cats.effect._

import org.broadinstitute.dig.aggregator.core.{Analysis, GraphDb, Provenance, Run}
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.{Processor, RunProcessor}

import org.neo4j.driver.v1.StatementResult

class UploadRegionsProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    GregorPipeline.sortRegionsProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Nil

  /** Name of the analysis node when uploading results.
    */
  private val analysisName: String = "ChromatinState/Regions"

  /** Generate the run output for the input results.
    */
  override def getRunOutputs(results: Seq[Run.Result]): Map[String, Seq[String]] = {
    Map(analysisName -> results.map(_.output).distinct)
  }

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val graph    = new GraphDb(config.neo4j)
    val analysis = new Analysis(analysisName, Provenance.thisBuild)
    val s3path   = "out/gregor/overlapped-variants/chromatin_state/"

    val io = for {
      id <- analysis.create(graph)

      // find and upload all the sorted region part files
      _ <- analysis.uploadParts(aws, graph, id, s3path)(uploadPart)
    } yield ()

    // ensure that the graph connection is closed
    io.guarantee(graph.shutdown())
  }

  /** Upload each individual part file.
    */
  def uploadPart(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    for {
      _      <- createAnnotations(graph, part)
      result <- uploadRegions(graph, id, part)
    } yield result
  }

  /** The name of each part file has the tissue and annotation in it, need to ensure
    * the annotation is present in the database before creating the regions linking
    * to it.
    */
  def createAnnotations(graph: GraphDb, part: String): IO[StatementResult] = {
    val q = s"""|LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// create the annotation if it doesn't exist
                |MERGE (a:Annotation {name: r.name})
                |ON CREATE SET
                |  a.type = 'ChromatinState'
                |""".stripMargin

    graph.run(q)
  }

  /** Create all the region nodes.
    */
  def uploadRegions(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 10000
                |LOAD CSV FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |
                |// lookup the tissue and annotation to connect
                |MATCH (t:Tissue {name: r.biosample})
                |MATCH (a:Annotation {name: r.name})
                |
                |// create the result node
                |CREATE (n:Region {
                |  chromosome: r.chromosome,
                |  start: toInteger(r.start),
                |  end: toInteger(r.stop),
                |  score: toInteger(t.score),
                |  itemRgb: r.itemRgb,
                |})
                |
                |// create the relationships
                |CREATE (q)-[:PRODUCED]->(n)
                |CREATE (t)-[:HAS_REGION]->(n)
                |CREATE (n)-[:HAS_ANNOTATION]->(a)
                |
                |// find all variants in this region
                |MATCH (v:Variant) WHERE v.name IN r.overlappedVariants
                |
                |// create a relationship to the region
                |CREATE (v)-[:HAS_REGION]->(n)
                |""".stripMargin

    graph.run(q)
  }
}
