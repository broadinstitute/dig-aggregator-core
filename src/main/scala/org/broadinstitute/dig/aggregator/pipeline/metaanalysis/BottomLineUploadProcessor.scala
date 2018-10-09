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
 * When trans-ethnic meta-analysis has been complete, in S3 output are
 * the bottom line results calculated for each of the variants for a given
 * phenotype.
 *
 * This processor will take those tables, and create :Frequency nodes in the
 * graph database, deleting all the existing frequencies for each phenotype/
 * ancestry pair per variant first.
 *
 * The source tables are read from:
 *
 *  s3://dig-analysis-data/out/metaanalysis/<phenotype>/trans-ethnic
 */
class BottomLineUploadProcessor(config: BaseConfig) extends RunProcessor(config) {

  /**
   * Unique name identifying this processor.
   */
  val name: Processor.Name = Processors.bottomLineUploadProcessor

  /**
   * All the processors this processor depends on.
   */
  val dependencies: Seq[Processor.Name] = Seq(
    Processors.ancestrySpecificProcessor,
  )

  /**
   * All the job scripts that need to be uploaded to AWS.
   */
  val resources: Seq[String] = Nil

  /**
   * Neo4j connection pool.
   */
  val driver: Driver = config.neo4j.newDriver

  /**
   * Take all the phenotype results from the dependencies and process them.
   */
  def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val phenotypes = results.map(_.output).distinct

    // create runs for every phenotype
    val ios = for (phenotype <- phenotypes) yield {
      val analysis = new Analysis(s"TransEthnic-MetaAnalysis/$phenotype", Provenance())

      for {
        _ <- IO(logger.info(s"Uploading trans-ethnic bottom-line results for $phenotype..."))

        // find all the part files to upload for the analysis
        _ <- analysis.uploadParts(aws, driver, s"out/metaanalysis/$phenotype/trans-ethnic/")(upload)

        // add the result to the database
        _ <- Run.insert(xa, name, Seq(phenotype), analysis.name)
        _ <- IO(logger.info("Done"))
      } yield ()
    }

    // process each phenotype serially
    ios.toList.sequence >> IO.unit
  }

  /**
   * Given a part file, upload it and create all the frequency nodes.
   */
  def upload(analysisNodeId: Int, part: String): StatementResult = {
    val q = s"""|USING PERIODIC COMMIT
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$analysisNodeId
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
                |  v.allele=r.allele
                |
                |// create the BottomLine analysis node
                |CREATE (n:BottomLine {
                |  pValue: toFloat(r.pValue),
                |  beta: toFloat(r.beta),
                |  maxEAF: toFloat(r.maxFreq),
                |  stdErr: toFloat(r.stderr),
                |  sampleSize: toInteger(r.sampleSize)
                |})
                |
                |// create the relationship to the analysis node
                |CREATE (q)<-[:PRODUCED_BY]-(n)
                |
                |// create the relationship to the trait and variant
                |CREATE (p)<-[:FOR_PHENOTYPE]-(n)-[:FOR_VARIANT]->(v)
                |
                |// if a top result, mark it as such
                |FOREACH(i IN (CASE toBoolean(r.top) WHEN true THEN [1] ELSE [] END) |
                |  CREATE (v)<-[:TOP_VARIANT]-(n)
                |)
                |""".stripMargin

    driver.session.run(q)
  }
}
