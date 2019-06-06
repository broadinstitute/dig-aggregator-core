package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors._

import org.neo4j.driver.v1.StatementResult

/** When meta-analysis has been complete, in S3 output are the bottom line
  * results calculated for each of the variants for a given phenotype.
  *
  * This processor will take the output from the ancestry-specific analysis and
  * creates :Frequency nodes in the graph database, and then take the results
  * of the trans-ethnic analysis and create :MetaAnalysis nodes.
  *
  * The source tables are read from:
  *
  *  s3://dig-analysis-data/out/metaanalysis/<phenotype>/ancestry-specific
  *  s3://dig-analysis-data/out/metaanalysis/<phenotype>/trans-ethnic
  */
class UploadMetaAnalysisProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    MetaAnalysisPipeline.metaAnalysisProcessor
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Nil

  /** Create the mapping of outputs -> inputs.
    */
  override def getRunOutputs(results: Seq[Run.Result]): Map[String, Seq[String]] = {
    results
      .map(_.output)
      .distinct
      .map { p =>
        s"MetaAnalysis/$p" -> Seq(p)
      }
      .toMap
  }

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val graph = new GraphDb(config.neo4j)

    // upload the results of each phenotype
    val ios = for ((output, Seq(phenotype)) <- getRunOutputs(results)) yield {
      val analysis   = new Analysis(output, Provenance.thisBuild)
      val bottomLine = s"out/metaanalysis/trans-ethnic/$phenotype/"

      for {
        id <- analysis.create(graph)

        // find and upload all the bottom-line result part files
        _ <- analysis.uploadParts(aws, graph, id, bottomLine)(uploadResults)
      } yield ()
    }

    // process each phenotype serially
    ios.toList.sequence.as(()).guarantee(graph.shutdown())
  }

  /** Given a part file, upload it and create all the bottom-line nodes.
    */
  def uploadResults(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    for {
      _       <- mergeVariants(graph, part)
      results <- createResults(graph, id, part)
    } yield results
  }

  /** Ensure the variants for each result exist.
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

  /** Create all the result nodes and relationships.
    */
  def createResults(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 50000
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |MATCH (p:Phenotype {name: r.phenotype})
                |MATCH (v:Variant {name: r.varId})
                |
                |// create the result node
                |CREATE (n:MetaAnalysis {
                |  pValue: toFloat(r.pValue),
                |  beta: toFloat(r.beta),
                |  stdErr: toFloat(r.stdErr),
                |  zScore: toFloat(r.zScore),
                |  n: toInteger(r.n)
                |})
                |
                |// create the relationship to the analysis node
                |CREATE (q)-[:PRODUCED]->(n)
                |
                |// create the relationship to the trait and variant
                |CREATE (p)-[:HAS_META_ANALYSIS]->(n)<-[:HAS_META_ANALYSIS]-(v)
                |
                |// if a top result, mark it as such
                |FOREACH(i IN (CASE toBoolean(r.top) WHEN true THEN [1] ELSE [] END) |
                |  CREATE (v)-[:TOP_VARIANT]->(n)
                |)
                |""".stripMargin

    graph.run(q)
  }
}
