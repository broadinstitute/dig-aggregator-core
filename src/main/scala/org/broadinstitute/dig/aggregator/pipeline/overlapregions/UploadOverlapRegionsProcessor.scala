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
    val s3path   = "out/overlapregions/"

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
      _      <- uploadOverlappedRegions(graph, id, part)
      result <- uploadRegions(graph, id, part)
    } yield result
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
                |// lookup the tissue and overlapped region
                |MATCH (t:Tissue {name: r.tissue})
                |MATCH (o:OverlappedRegion {name: r.overlappedRegion})
                |
                |// create the region node
                |CREATE (n:Region {
                |  name: r.name,
                |  annotation: r.annotation,
                |  chromosome: r.chromosome,
                |  start: toInteger(r.start),
                |  end: toInteger(r.end),
                |  rgb: r.itemRgb
                |})
                |
                |// create the required relationships
                |CREATE (q)-[:PRODUCED]->(n)
                |CREATE (n)-[:OVERLAPS]->(o)
                |CREATE (t)-[:HAS_REGION]->(n)
                |""".stripMargin

    graph.run(q)
  }

  /** Create all the region nodes.
    */
  def uploadOverlappedRegions(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 10000
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |
                |// split the name into chromosome and start position
                |WITH q, r, split(r.overlappedRegion, ':') AS locus
                |WITH q, r, locus[0] AS chromosome, split(locus[1], '-') AS position
                |
                |// lookup the tissue and annotation to connect
                |MERGE (n:OverlappedRegion {name: r.overlappedRegion})
                |ON CREATE SET
                |  n.chromosome=chromosome,
                |  n.start=toInteger(position[0]),
                |  n.end=toInteger(position[1])
                |
                |// connect to the analysis node
                |MERGE (q)-[:PRODUCED]->(n)
                |""".stripMargin

    graph.run(q)
  }
}
