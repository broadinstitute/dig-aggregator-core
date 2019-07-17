package org.broadinstitute.dig.aggregator.pipeline.frequencyanalysis

import cats.effect._
import cats.implicits._

import java.util.UUID

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig

import org.neo4j.driver.v1.StatementResult

/** This processor will take the output from the frequency analysis and
  * creates :Frequency nodes in the graph database.
  *
  * The source tables are read from:
  *
  *  s3://dig-analysis-data/out/frequencyanalysis/<phenotype>/part-*
  */
class UploadFrequencyAnalysisProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    FrequencyAnalysisPipeline.frequencyProcessor
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Nil

  /** Build the list of results to upload to the database.
    */
  override def getRunOutputs(results: Seq[Run.Result]): Map[String, Seq[UUID]] = {
    val phenotypes = results.map { run =>
      run.output -> run.uuid
    }

    phenotypes
      .groupBy(_._1)
      .map {
        case (phenotype, phenotypeInputPairs) =>
          s"FrequencyAnalysis/$phenotype" -> phenotypeInputPairs.map(_._2).distinct
      }
  }

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val runMap = getRunOutputs(results)

    // upload the results of each phenotype
    val ios = for ((output, Seq(phenotype)) <- runMap) yield {
      val analysis = new Analysis(output, Provenance.thisBuild)
      val graph    = new GraphDb(config.neo4j)

      // where the result files are to upload
      val bottomLine = s"out/frequencyanalysis/$phenotype/"

      val io = for {
        id <- analysis.create(graph)

        // find and upload all the bottom-line result part files
        _ <- analysis.uploadParts(aws, graph, id, bottomLine)(uploadResults)
      } yield ()

      // ensure the connection to the database is closed
      io.guarantee(graph.shutdown())
    }

    // process each phenotype serially
    ios.toList.sequence.as(())
  }

  /**
    * Given a part file, upload it and create all the bottom-line nodes.
    */
  def uploadResults(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    for {
      _       <- mergeVariants(graph, part)
      results <- createResults(graph, id, part)
    } yield results
  }

  /**
    * Ensure the variants for each result exist.
    */
  def mergeVariants(graph: GraphDb, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 50000
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// create the variant node if it doesn't exist
                |MERGE (v:Variant {name: r.varId})
                |ON CREATE SET
                |  v.chromosome=r.chromosome,
                |  v.position=toInteger(r.position),
                |  v.reference=r.reference,
                |  v.alt=r.alt
                |""".stripMargin

    graph.run(q)
  }

  /**
    * Create all the result nodes and relationships.
    */
  def createResults(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 50000
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |
                |// skip if the phenotype, ancestry, or variant don't exist
                |MATCH (p:Phenotype {name: r.phenotype})
                |MATCH (a:Ancestry {name: r.ancestry})
                |MATCH (v:Variant {name: r.varId})
                |
                |// create the frequency result node
                |CREATE (n:Frequency {
                |  eaf: toFloat(r.eaf),
                |  maf: toFloat(r.maf)
                |})
                |
                |// create the relationship to the analysis node
                |CREATE (q)-[:PRODUCED]->(n)
                |
                |// create the relationship to the trait, ancestry, and variant
                |CREATE (p)-[:HAS_FREQUENCY]->(n)
                |CREATE (v)-[:HAS_FREQUENCY]->(n)
                |CREATE (a)-[:HAS_FREQUENCY]->(n)
                |""".stripMargin

    graph.run(q)
  }
}
