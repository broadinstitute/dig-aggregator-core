package org.broadinstitute.dig.aggregator.pipeline.overlapregions

import cats.effect._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.pipeline.genepredictions.GenePredictionsPipeline
import org.broadinstitute.dig.aggregator.pipeline.gregor.GregorPipeline
import org.broadinstitute.dig.aggregator.pipeline.varianteffect.VariantEffectPipeline
import org.neo4j.driver.v1.StatementResult

class UploadOverlapRegionsProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    OverlapRegionsPipeline.overlapRegionsProcessor,
    // Being dependent on other uploads guarantees that the nodes that will
    // be linked to will exist when this data is uploaded.
    VariantEffectPipeline.uploadVariantCQSProcessor,
    GregorPipeline.uploadAnnotatedRegionsProcessor,
    //GenePredictionsPipeline.uploadGenePredictionsProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Nil

  /** Name of the analysis node when uploading results.
    */
  private val analysisName: String = "OverlapRegions"

  /** All inputs are uploaded into a single output.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq(analysisName))
  }

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val graph            = new GraphDb(config.neo4j)
    val analysis         = new Analysis(analysisName, Provenance.thisBuild)
    val unique           = "out/overlapregions/unique/"
    val annotatedRegions = "out/overlapregions/overlapped/annotated_regions/"
    val genePredictions  = "out/overlapregions/overlapped/gene_predictions/"
    val variants         = "out/overlapregions/variants/"

    val io = for {
      id <- analysis.create(graph)

      // find and upload all the sorted region part files
      _ <- IO(logger.info("Uploading unique OverlapRegion nodes..."))
      _ <- analysis.uploadParts(aws, graph, id, unique)(uploadUniqueOverlapRegions)
      _ <- IO(logger.info("Uploading OverlapRegion relationships to AnnotatedRegions..."))
      _ <- analysis.uploadParts(aws, graph, id, annotatedRegions)(uploadAnnotatedRegionRelationships)
      _ <- IO(logger.info("Uploading OverlapRegion relationships to GenePredictions..."))
      _ <- analysis.uploadParts(aws, graph, id, genePredictions)(uploadGenePredictionRelationships)
      _ <- IO(logger.info("Uploading OverlapRegion relationships to Variants..."))
      _ <- analysis.uploadParts(aws, graph, id, variants)(uploadVariantRelationships)
    } yield ()

    // ensure that the graph connection is closed
    io.guarantee(graph.shutdown())
  }

  /** Create all the overlap region nodes.
    */
  def uploadUniqueOverlapRegions(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 10000
                |LOAD CSV WITH HEADERS FROM '$part' AS row
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |
                |// create the OverlapRegion node
                |CREATE (n:OverlapRegion {
                |  name: row.name,
                |  chromosome: row.chromosome,
                |  start: toInteger(row.start),
                |  end: toInteger(row.end)
                |})
                |
                |// create relationship to analysis
                |CREATE (q)-[:PRODUCED]->(n)
                |""".stripMargin

    graph.run(q)
  }

  /** Actually create the relationships to regions and annotated regions.
    */
  def uploadAnnotatedRegionRelationships(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 10000
                |LOAD CSV WITH HEADERS FROM '$part' AS row
                |FIELDTERMINATOR '\t'
                |
                |// lookup the overlap node and annotated region
                |MATCH (n:OverlapRegion {name: row.name})
                |MATCH (r:AnnotatedRegion {name: row.region})
                |
                |// create the required relationships
                |CREATE (n)-[:OVERLAPS_ANNOTATED_REGION]->(r)
                |""".stripMargin

    graph.run(q)
  }

  /** Actually create the relationships to regions and gene predictions.
    */
  def uploadGenePredictionRelationships(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 10000
                |LOAD CSV WITH HEADERS FROM '$part' AS row
                |FIELDTERMINATOR '\t'
                |
                |// lookup the overlap node and annotated region
                |MATCH (n:OverlapRegion {name: row.name})
                |MATCH (r:GenePrediction {name: row.region})
                |
                |// create the required relationships
                |CREATE (n)-[:OVERLAPS_GENE_PREDICTION]->(r)
                |""".stripMargin

    graph.run(q)
  }

  /** Link overlapped regions to variants.
    */
  def uploadVariantRelationships(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 10000
                |LOAD CSV WITH HEADERS FROM '$part' AS row
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node and annotated region
                |MATCH (n:OverlapRegion {name: row.name})
                |MATCH (v:Variant {name: row.varId})
                |
                |// create the required relationships
                |CREATE (n)-[:OVERLAPS_VARIANT]->(v)
                |""".stripMargin

    graph.run(q)
  }
}
