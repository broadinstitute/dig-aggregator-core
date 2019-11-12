package org.broadinstitute.dig.aggregator.pipeline.overlapregions

import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core.{Analysis, GraphDb, Processor, Provenance, Run}
import org.broadinstitute.dig.aggregator.core.config.BaseConfig

import org.neo4j.driver.v1.StatementResult
import org.broadinstitute.dig.aggregator.core.DbPool

/** This processor exists and is very different from the others. Technically it is
  * after the UploadOverlapRegionsProcessor, but it exists solely to create the
  * wealth of relationships from :OverlapRegion nodes to the various node types that
  * they overlap.
  *
  * The :Analysis nodes won't have any nodes that they :PRODUCED in the graph, and
  * merely exist to show that the relationships exist. Similarly, this means the
  * relationships won't be deleted by deleting the analysis nodes, but rather are
  * done by the upload. However, in practice this isn't often necessary because the
  * processor dependency (UploadRegions) already deleted them all when it deleted
  * all the :OverlapRegion nodes. The delete here is just in case it needs to be
  * rerun for whatever reason.
  *
  * Lastly, each of the type of relationship that is created (read: the target node)
  * is a separate output so that the CLI can be given the `--only` flag to limit the
  * work that needs to be done by this processor.
  */
class CreateOverlapRegionRelationshipsProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool)
    extends Processor(name, config, pool) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    OverlapRegionsPipeline.uploadOverlapRegionsProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Nil

  /** Name of the analysis node when uploading results.
    */
  private val analysisName: String = "OverlapRegions"

  /** Multiple outputs are produced from the single input.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    val relationships = Seq(
      "OverlapRegions/AnnotatedRegions",
      "OverlapRegions/Variants",
    )

    Processor.Outputs(relationships)
  }

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val graph = new GraphDb(config.neo4j)

    // source locations for the relationships
    val annotatedRegions = "out/overlapregions/overlapped/annotated_regions/"
    val variants         = "out/overlapregions/overlapped/variants/"

    // descriptions of the work to do
    val ios: Seq[IO[Unit]] = outputs.map {
      case output @ "OverlapRegions/AnnotatedRegions" =>
        processOutput(graph, output, "OVERLAPS_ANNOTATED_REGION", annotatedRegions, createAnnotatedRegionRelationships)
      case output @ "OverlapRegions/Variants" =>
        processOutput(graph, output, "OVERLAPS_VARIANT", variants, createVariantRelationships)
    }

    // ensure that the graph connection is closed
    ios.toList.sequence.guarantee(graph.shutdown()).as(())
  }

  /** Creates the IO job that will delete existing relationships and create new ones.
    */
  def processOutput(graph: GraphDb,
                    output: String,
                    relationship: String,
                    parts: String,
                    create: (GraphDb, Long, String) => IO[StatementResult]): IO[Unit] = {
    val analysis = new Analysis(output, Provenance.thisBuild)

    for {
      id <- analysis.create(graph)

      // delete all the existing relationships first
      _ <- IO(logger.info(s"Deleting existing :${relationship} relationships from OverlapRegions..."))
      _ <- deleteRelationships(graph, relationship)

      // creating the new relationships
      _ <- IO(logger.info(s"Creating new :${relationship} relationships from OverlapRegions..."))
      _ <- analysis.uploadParts(aws, graph, id, parts)(create)
    } yield ()
  }

  /** Delete existing relationships of a given type.
    */
  def deleteRelationships(graph: GraphDb, relationship: String): IO[StatementResult] = {
    val q =
      s"""|CALL apoc.periodic.iterate(
          |  "MATCH (:OverlapRegion)-[x:$relationship]->() RETURN x",
          |  "DELETE x",
          |  {batchSize: 1000, parallel: false}
          |)
          |""".stripMargin

    graph.run(q)
  }

  /** Create the relationships to annotated regions.
    */
  def createAnnotatedRegionRelationships(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
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

  /** Create the relationships to variants.
    */
  def createVariantRelationships(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
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
