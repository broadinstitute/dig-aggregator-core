package org.broadinstitute.dig.aggregator.pipeline.genepredictions

import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.{Analysis, GraphDb, Processor, Provenance, Run}
import org.neo4j.driver.v1.StatementResult
import org.broadinstitute.dig.aggregator.core.DbPool

class UploadGenePredictionsProcessor(name: Processor.Name, config: BaseConfig, pool: DbPool) extends Processor(name, config, pool) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    GenePredictionsPipeline.genePredictionsProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Nil

  /** The input phenotype is also the output.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq(input.output))
  }

  /** Take all the phenotype results from the dependencies and upload them.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val graph    = new GraphDb(config.neo4j)
    val analysis = new Analysis(s"GenePredictions/Regions", Provenance.thisBuild)
    val parts    = s"out/genepredictions/regions/"

    for {
      id <- analysis.create(graph)
      _  <- analysis.uploadParts(aws, graph, id, parts)(uploadRegions)
    } yield ()
  }

  /** Given a part file, upload it and create all the regions.
    */
  def uploadRegions(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 10000
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node and the gene
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |MATCH (g:Gene {ensemblId: r.predictedTargetGene})
                |
                |// create the region name
                |WITH q, g, r, (r.chromosome + ':' + r.start + '-' + r.end) AS name
                |
                |// create the region node if it doesn't exist
                |MERGE (n:GenePrediction {name: name})
                |ON CREATE SET
                |  n.chromosome=r.chromosome,
                |  n.start=toInteger(r.start),
                |  n.end=toInteger(r.end),
                |  n.targetStart=toInteger(r.targetStart),
                |  n.targetEnd=toInteger(r.targetEnd),
                |  n.value=toFloat(r.value)
                |
                |// create relationships
                |MERGE (q)-[:PRODUCED]->(n)
                |MERGE (n)-[:HAS_TARGET_GENE]->(g)
                |""".stripMargin

    graph.run(q)
  }
}
