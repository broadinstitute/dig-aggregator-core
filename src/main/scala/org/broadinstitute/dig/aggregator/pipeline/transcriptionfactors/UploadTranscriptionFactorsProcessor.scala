package org.broadinstitute.dig.aggregator.pipeline.transcriptionfactors

import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig

import org.neo4j.driver.v1.StatementResult

class UploadTranscriptionFactorsProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool) extends Processor(name, config, pool) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    TranscriptionFactorsPipeline.transcriptionFactorsProcessor
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Nil

  /** All inputs are uploaded into a single output.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq("TranscriptionFactors"))
  }

  /** There is only ever a single output.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val graph    = new GraphDb(config.neo4j)
    val analysis = new Analysis(s"TranscriptionFactors", Provenance.thisBuild)
    val parts    = s"out/transcriptionfactors/"

    val io = for {
      id <- analysis.create(graph)
      _  <- IO(logger.info(s"Uploading transcription factors..."))
      _  <- analysis.uploadParts(aws, graph, id, parts)(uploadParts)
    } yield ()

    // process each phenotype serially
    io.guarantee(graph.shutdown())
  }

  /** Create all the overlap region nodes.
    */
  def uploadParts(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 10000
                |LOAD CSV WITH HEADERS FROM '$part' AS row
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |MATCH (v:Variant {name: row.varId})
                |
                |// create the OverlapRegion node
                |CREATE (n:TranscriptionFactor {
                |  positionWeightMatrix: row.positionWeightMatrix,
                |  delta: toFloat(row.delta),
                |  position: toInteger(row.position),
                |  strand: row.strand,
                |  refScore: toFloat(row.refScore),
                |  altScore: toFloat(row.altScore)
                |})
                |
                |// create relationship to analysis
                |CREATE (q)-[:PRODUCED]->(n)
                |CREATE (v)-[:HAS_TRANSCRIPTION_FACTOR]->(n)
                |""".stripMargin

    graph.run(q)
  }
}
