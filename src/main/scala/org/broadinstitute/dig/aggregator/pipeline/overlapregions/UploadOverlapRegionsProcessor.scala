package org.broadinstitute.dig.aggregator.pipeline.overlapregions

import cats.effect._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core._
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
    val graph    = new GraphDb(config.neo4j)
    val analysis = new Analysis(analysisName, Provenance.thisBuild)
    val regions  = "out/overlapregions/regions/"
    val variants = "out/overlapregions/variants/"

    val io = for {
      id <- analysis.create(graph)

      // find and upload all the sorted region part files
      _ <- analysis.uploadParts(aws, graph, id, regions)(uploadOverlappedRegions)
      _ <- analysis.uploadParts(aws, graph, id, variants)(uploadOverlappedVariants)
    } yield ()

    // ensure that the graph connection is closed
    io.guarantee(graph.shutdown())
  }

  /** Link overlapped regions to regions.
    */
  def uploadOverlappedRegions(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 10000
                |LOAD CSV WITH HEADERS FROM '$part' AS row
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |
                |// lookup the region being overlapped
                |MATCH (r:Region {name: row.region})
                |
                |// create the region node
                |MERGE (n:OverlappedRegion {name: row.name})
                |ON CREATE SET
                |  n.chromosome=row.chromosome,
                |  n.start=toInteger(row.start),
                |  n.end=toInteger(row.end)
                |
                |// create the required relationships
                |CREATE (q)-[:PRODUCED]->(n)
                |CREATE (n)-[:OVERLAPS_REGION]->(r)
                |""".stripMargin

    graph.run(q)
  }

  /** Link overlapped regions to variants.
    */
  def uploadOverlappedVariants(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 10000
                |LOAD CSV WITH HEADERS FROM '$part' AS row
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |
                |// lookup the region being overlapped
                |MATCH (v:Variant {name: row.varId})
                |
                |// create the region node
                |MERGE (n:OverlappedRegion {name: row.name})
                |ON CREATE SET
                |  n.chromosome=row.chromosome,
                |  n.start=toInteger(row.start),
                |  n.end=toInteger(row.end)
                |
                |// create the required relationships
                |CREATE (q)-[:PRODUCED]->(n)
                |CREATE (n)-[:OVERLAPS_VARIANT]->(v)
                |""".stripMargin

    graph.run(q)
  }
}
