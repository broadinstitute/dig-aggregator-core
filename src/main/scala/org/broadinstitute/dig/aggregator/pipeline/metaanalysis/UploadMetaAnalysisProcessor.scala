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
    MetaAnalysisPipeline.metaAnalysisProcessor,
  )

  /**
   * All the job scripts that need to be uploaded to AWS.
   */
  override val resources: Seq[String] = Nil

  /**
   * When loading the CSV files from HDFS, they don't have a header attached
   * to them, so this is the definitive ordering of the columns a
   */
  private val fields: Seq[String] = Array[String](
    "varId",
    "chromosome",
    "position",
    "reference",
    "alt",
    "phenotype",
    "pValue",
    "beta",
    "eaf",
    "maf",
    "stdErr",
    "n",
    "top",
  )

  /**
   * Create a column mapper for a given row variable.
   */
  private def columnMapper(name: String) = new Object {
    def apply(col: String) = s"$name[${fields.indexOf(col)}]"
  }

  /**
   * Take all the phenotype results from the dependencies and process them.
   */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val phenotypes = results.map(_.output).distinct

    // create runs for every phenotype
    val ios = for (phenotype <- phenotypes) yield {
      val analysis = new Analysis(s"MetaAnalysis/$phenotype", Provenance.thisBuild)
      val driver   = config.neo4j.newDriver

      // the ancestries for each phenotype
      val ancestries = Seq("AA", "AF", "EU", "HS", "EA", "SA")

      // for each ancestry, collect upload the part files for it
      def frequencies(id: Int) = for (ancestry <- ancestries) yield {
        val parts  = s"out/metaanalysis/ancestry-specific/$phenotype/ancestry=$ancestry"
        analysis.uploadParts(aws, driver, id, parts)(uploadFrequencyResults(ancestry))
      }

      // where the result files are to upload
      val bottomLine = s"out/metaanalysis/trans-ethnic/$phenotype"

      for {
        _ <- IO(logger.info(s"Preparing upload of $phenotype meta-analysis..."))

        // delete the existing analysis and recreate it
        id <- analysis.create(driver)

        // find all the part files to upload for the analysis
        _ <- IO(logger.info(s"Uploading frequencies for $phenotype..."))
        _ <- frequencies(id).toList.sequence

        // find and upload all the bottom-line result part files
        _ <- IO(logger.info(s"Uploading bottom-line results for $phenotype..."))
        _ <- analysis.uploadParts(aws, driver, id, bottomLine)(uploadBottomLineResults)

        // add the result to the database
        _ <- Run.insert(pool, name, Seq(phenotype), analysis.name)
        _ <- IO(logger.info("Done"))
      } yield ()
    }

    // process each phenotype serially
    ios.toList.sequence >> IO.unit
  }

  /**
   * Given a part file, upload it and create all the frequency nodes.
   */
  def uploadFrequencyResults(ancestry: String)(driver: Driver, id: Int, part: String): StatementResult = {
    import scala.language.reflectiveCalls

    val r = columnMapper("r")
    val q = s"""|USING PERIODIC COMMIT
                |LOAD CSV FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |
                |// die if the ancestry or phenotype doesn't exist
                |MATCH (a:Ancestry {name: '$ancestry'})
                |MATCH (p:Phenotype {name: ${r("phenotype")}})
                |
                |// create the variant node if it doesn't exist
                |MERGE (v:Variant {name: ${r("varId")}})
                |ON CREATE SET
                |  v.chromosome=${r("chromosome")},
                |  v.position=toInteger(${r("position")}),
                |  v.reference=${r("reference")},
                |  v.alt=${r("alt")}
                |
                |WITH q, a, p, v, toFloat(${r("eaf")}) AS eaf
                |
                |// create the Frequency node, calculate MAF
                |CREATE (n:Frequency {
                |  EAF: eaf,
                |  MAF: CASE WHEN eaf < 0.5 THEN eaf ELSE 1.0 - eaf END
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

  /**
   * Given a part file, upload it and create all the bottom-line nodes.
   */
  def uploadBottomLineResults(driver: Driver, id: Int, part: String): StatementResult = {
    import scala.language.reflectiveCalls

    val r = columnMapper("r")
    val q = s"""|USING PERIODIC COMMIT
                |LOAD CSV FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |
                |// die if the phenotype doesn't exist
                |MATCH (p:Phenotype {name: ${r("phenotype")}})
                |
                |// create the variant node if it doesn't exist
                |MERGE (v:Variant {name: ${r("varId")}})
                |ON CREATE SET
                |  v.chromosome=${r("chromosome")},
                |  v.position=toInteger(${r("position")}),
                |  v.reference=${r("reference")},
                |  v.alt=${r("alt")}
                |
                |// create the MetaAnalysis analysis node
                |CREATE (n:MetaAnalysis {
                |  pValue: toFloat(${r("pValue")}),
                |  beta: toFloat(${r("beta")}),
                |  stdErr: toFloat(${r("stdErr")}),
                |  n: toInteger(${r("n")})
                |})
                |
                |// create the relationship to the analysis node
                |CREATE (q)<-[:PRODUCED_BY]-(n)
                |
                |// create the relationship to the trait and variant
                |CREATE (p)<-[:FOR_PHENOTYPE]-(n)-[:FOR_VARIANT]->(v)
                |
                |// if a top result, mark it as such
                |FOREACH(i IN (CASE toBoolean(${r("top")}) WHEN true THEN [1] ELSE [] END) |
                |  CREATE (v)<-[:TOP_VARIANT]-(n)
                |)
                |""".stripMargin

    driver.session.run(q)
  }
}
