package org.broadinstitute.dig.aggregator.pipeline.transcriptionfactors

import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig

import org.neo4j.driver.v1.StatementResult

class UploadTranscriptionFactorsProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

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

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val graph = new GraphDb(config.neo4j)

    val ios = for (output <- outputs) yield {
      val analysis = new Analysis(s"TranscriptionFactors/${output}", Provenance.thisBuild)
      val parts    = s"out/transcription_factors/${output}/"

      for {
        id <- analysis.create(graph)
        _  <- IO(logger.info(s"Uploading transcription factors for variants of ${output}..."))
        _  <- analysis.uploadParts(aws, graph, id, parts)(uploadParts)
      } yield ()
    }

    // process each phenotype serially
    ios.toList.sequence.as(()).guarantee(graph.shutdown())
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
                |MATCH (v:Variant {name: r.varId})
                |
                |// create the OverlapRegion node
                |CREATE (n:TranscriptionFactor {
                |  positionWeightMatrix: r.positionWeightMatrix,
                |  delta: toFloat(r.delta),
                |  position: toInteger(r.position),
                |  strand: r.strand,
                |  refScore: toFloat(r.refScore),
                |  altScore: toFloat(r.altScore)
                |})
                |
                |// create relationship to analysis
                |CREATE (q)-[:PRODUCED]->(n)
                |CREATE (v)-[:HAS_TRANSCRIPTION_FACTOR]->(n)
                |""".stripMargin

    graph.run(q)
  }
}
