package org.broadinstitute.dig.aggregator.pipeline.metaanalysis

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors._

import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.StatementResult

/**
 * When meta-analysis has been complete, in S3 output are the bottom line
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

  /**
   * All the processors this processor depends on.
   */
  override val dependencies: Seq[Processor.Name] = Seq(
    MetaAnalysisPipeline.metaAnalysisProcessor
  )

  /**
   * All the job scripts that need to be uploaded to AWS.
   */
  override val resources: Seq[String] = Nil

  /**
   * Take all the phenotype results from the dependencies and process them.
   */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val phenotypes = results.map(_.output).distinct

    // create runs for every phenotype
    val ios = for (phenotype <- phenotypes) yield {
      val analysis = new Analysis(s"MetaAnalysis/$phenotype", Provenance.thisBuild)
      val graph    = new GraphDb(config.neo4j)

      // where the result files are to upload
      val bottomLine = s"out/metaanalysis/trans-ethnic/$phenotype/"

      for {
        _ <- IO(logger.info(s"Creating Analysis node..."))

        // delete the existing analysis and recreate it
        id <- analysis.create(graph)

        // find and upload all the bottom-line result part files
        _ <- IO(logger.info(s"Uploading results for $phenotype..."))
        _ <- analysis.uploadParts(aws, graph, id, bottomLine)(uploadResults)

        // add the result to the database
        _ <- IO(logger.info(s"Updating database..."))
        _ <- Run.insert(pool, name, Seq(phenotype), analysis.name)
        _ <- IO(logger.info("Done"))
      } yield ()
    }

    // process each phenotype serially
    ios.toList.sequence >> IO.unit
  }

  /**
   * Given a part file, upload it and create all the bottom-line nodes.
   */
  def uploadResults(graph: GraphDb, id: Int, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |
                |// die if the phenotype doesn't exist
                |MATCH (p:Phenotype {name: r.phenotype})
                |
                |// create the variant node if it doesn't exist
                |MERGE (v:Variant {name: r.varId})
                |ON CREATE SET
                |  v.chromosome=r.chromosome,
                |  v.position=toInteger(r.position),
                |  v.reference=r.reference,
                |  v.alt=r.alt
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
