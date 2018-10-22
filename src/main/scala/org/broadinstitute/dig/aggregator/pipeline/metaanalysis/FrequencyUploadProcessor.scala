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
 * When ancestry-specific meta-analysis has been complete, in S3 output are
 * the frequencies calculated for each of the variants for a given phenotype.
 *
 * This processor will take those tables, and create :Frequency nodes in the
 * graph database, deleting all the existing frequencies for each phenotype/
 * ancestry pair per variant first.
 *
 * The source tables are read from:
 *
 *  s3://dig-analysis-data/out/metaanalysis/<phenotype>/ancestry-specific/<ancestry>
 */
class FrequencyUploadProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

  /**
   * All the processors this processor depends on.
   */
  override val dependencies: Seq[Processor.Name] = Seq(
    MetaAnalysisPipeline.ancestrySpecificProcessor,
  )

  /**
   * All the job scripts that need to be uploaded to AWS.
   */
  override val resources: Seq[String] = Nil

  /**
   * Neo4j connection pool.
   */
  val driver: Driver = config.neo4j.newDriver

  /**
   * Take all the phenotype results from the dependencies and process them.
   */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val phenotypes = results.map(_.output).distinct

    // create runs for every phenotype
    val ios = for (phenotype <- phenotypes) yield {
      val analysis = new Analysis(s"AncestrySpecific-MetaAnalysis/$phenotype", Provenance.thisBuild)

      for {
        _ <- IO(logger.info(s"Uploading ancestry-specific variant frequencies for $phenotype..."))

        // find all the part files to upload for the analysis
        _ <- analysis.uploadParts(aws, driver, s"out/metaanalysis/$phenotype/ancestry-specific/")(upload)

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
                |// die if the ancestry doesn't exist
                |MATCH (a:Ancestry {name: r.ancestry})
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
                |WITH q, a, p, v, toFloat(r.freq) AS freq
                |
                |// create the Frequency node, calculate MAF
                |CREATE (n:Frequency {
                |  EAF: freq,
                |  MAF: CASE WHEN freq < 0.5 THEN freq ELSE 1.0 - freq END
                |})
                |
                |// create the link from the analysis to this node
                |CREATE (q)<-[:PRODUCED_BY]-(n)
                |
                |// create the relationships
                |CREATE (a)<-[:FOR_ANCESTRY]-(n)
                |CREATE (v)<-[:FOR_VARIANT]-(n)
                |CREATE (p)<-[:FOR_PHENOTYPE]-(n)
                |""".stripMargin

    driver.session.run(q)
  }
}
