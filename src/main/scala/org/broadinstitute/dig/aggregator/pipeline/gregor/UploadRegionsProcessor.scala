package org.broadinstitute.dig.aggregator.pipeline.gregor

import java.net.URL
import java.net.URLDecoder

import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core.{Analysis, GraphDb, Implicits, Provenance, Run}
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.{Processor, RunProcessor}
import org.broadinstitute.dig.aggregator.pipeline.metaanalysis.MetaAnalysisPipeline
import org.neo4j.driver.v1.StatementResult

class UploadRegionsProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {
  import Implicits._

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    GregorPipeline.sortRegionsProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Nil

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val graph    = new GraphDb(config.neo4j)
    val analysis = new Analysis(s"ChromatinState/Regions", Provenance.thisBuild)

    val io = for {
      id <- analysis.create(graph)

      // find and upload all the sorted region part files
      _ <- analysis.uploadParts(aws, graph, id, "out/gregor/regions/chromatin_state/")(uploadPart)

      // add the result to the database
      _ <- Run.insert(pool, name, results.map(_.output).distinct, analysis.name)
      _ <- IO(logger.info("Done"))
    } yield ()

    // ensure that the graph connection is closed
    io.guarantee(graph.shutdown())
  }

  /** Extract the tissue and annotation names from the HDFS path.
    */
  def parseTissueAndAnnotation(part: String): (String, String) = {
    val pattern = raw".*/biosample=([^/]+)/name=([^/]+)/.*".r
    val path    = URLDecoder.decode(new URL(part).getPath, "UTF-8")

    // NOTE: When Spark ran on on the tissues, to prevent odd characters, we replaced
    //       ':' with '_', and here we'll put it back.
    path match {
      case pattern(tissue, annotation) =>
        (tissue.replaceFirst("_", ":"), annotation)
    }
  }

  /** Upload each individual part file.
    */
  def uploadPart(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    for {
      _      <- createAnnotation(graph, part)
      result <- uploadRegions(graph, id, part)
    } yield result
  }

  /** The name of each part file has the tissue and annotation in it, need to ensure
    * the annotation is present in the database before creating the regions linking
    * to it.
    */
  def createAnnotation(graph: GraphDb, part: String): IO[StatementResult] = {
    val (_, annotation) = parseTissueAndAnnotation(part)

    // create the annotation if it doesn't already exist
    val q = s"""|MERGE (a:Annotation {name: '$annotation'})
                |ON CREATE SET
                |  a.type = 'ChromatinState'
                |""".stripMargin

    graph.run(q)
  }

  /** Create all the region nodes.
    */
  def uploadRegions(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val (tissue, annotation) = parseTissueAndAnnotation(part)

    val q = s"""|USING PERIODIC COMMIT
                |LOAD CSV FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// parse the row fields
                |WITH r[0] AS chromosome, toInteger(r[1]) AS start, toInteger(r[2]) AS stop
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |MATCH (t:Tissue {name: '$tissue'})
                |MATCH (a:Annotation {name: '$annotation'})
                |
                |// create the result node (note the second label!)
                |CREATE (n:Region {
                |  chromosome: chromosome,
                |  start: start,
                |  end: stop
                |})
                |
                |// create the relationships
                |CREATE (q)-[:PRODUCED]->(n)
                |CREATE (t)-[:HAS_REGION]->(n)
                |CREATE (n)-[:HAS_ANNOTATION]->(a)
                |
                |// find all variants in this region
                |MATCH (v:Variant)
                |WHERE v.chromosome = chromosome
                |AND v.position >= start
                |AND v.end < stop
                |
                |// create a relationship to the region
                |CREATE (v)-[:HAS_REGION]->(n)
                |""".stripMargin

    graph.run(q)
  }

  def connectVariants(graph: GraphDb /*, chromosome: String*/ ): IO[StatementResult] = {
    val s = s"""|CYPHER runtime=slotted
                |MATCH (r:RegionToBeProcessed)
                |RETURN r
                |""".stripMargin

    // for each region in the stream...
    val q = s"""|//REMOVE r:RegionToBeProcessed
                |//WITH r
                |
                |// find all variants within this region
                |MATCH (v:Variant)
                |WHERE v.chromosome = r.chromosome
                |AND v.position >= r.start
                |AND v.position < r.end
                |
                |// connect the variant to the region
                |MERGE (v)-[:HAS_REGION]->(r)
                |""".stripMargin

    // run the query using the APOC function
    graph.run(s"call apoc.periodic.iterate('$s', '$q', {parallel: true})")
  }
}
