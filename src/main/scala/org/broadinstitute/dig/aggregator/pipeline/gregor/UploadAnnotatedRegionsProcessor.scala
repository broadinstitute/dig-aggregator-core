package org.broadinstitute.dig.aggregator.pipeline.gregor

import cats.effect._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core._
import org.neo4j.driver.v1.StatementResult

class UploadAnnotatedRegionsProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

  /** All the processors this processor depends on. Run after uploading the
    * global enrichment nodes so they can be linked to.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    GregorPipeline.sortRegionsProcessor,
    GregorPipeline.uploadGlobalEnrichmentProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Nil

  /** Name of the analysis node when uploading results.
    */
  private val analysisName: String = "GREGOR/AnnotatedRegions"

  /** All inputs are uploaded into a single output.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq(analysisName))
  }

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val graph    = new GraphDb(config.neo4j)
    val analysis = new Analysis(analysisName, Provenance.thisBuild)
    val s3path   = "out/gregor/regions/unsorted/"

    val io = for {
      id <- analysis.create(graph)

      // find and upload all the sorted region part files
      _ <- analysis.uploadParts(aws, graph, id, s3path)(uploadRegions)
    } yield ()

    // ensure that the graph connection is closed
    io.guarantee(graph.shutdown())
  }

  /** Create all the region nodes.
    */
  def uploadRegions(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 10000
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |
                |// join columns to make region name
                |WITH q, r, r.chromosome + ':' + r.start + '-' + r.end AS name
                |
                |// create the region node
                |MERGE (n:Region {name: name})
                |ON CREATE SET
                |  n.chromosome=r.chromosome,
                |  n.start=toInteger(r.start),
                |  n.end=toInteger(r.end)
                |
                |// create the required relationships
                |MERGE (q)-[:PRODUCED]->(n)
                |WITH r, n, replace(r.biosample, '_', ':') AS t
                |
                |// find global enrichment nodes to link
                |OPTIONAL MATCH (:Tissue {name: t})-[:HAS_GLOBAL_ENRICHMENT]->(g:GlobalEnrichment)
                |WHERE g.annotation=r.annotation AND g.method=r.method
                |
                |// join them into a list
                |WITH r, n, collect(g) AS gs
                |
                |// connect to all the enrichment nodes
                |FOREACH(g IN gs | CREATE (n)-[:HAS_GLOBAL_ENRICHMENT {rgb: r.itemIgb}]->(g))
                |""".stripMargin

    graph.run(q)
  }
}
