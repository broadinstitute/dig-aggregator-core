package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors._

import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.StatementResult

/**
 * When variant effect processing has been complete, in S3 output are the
 * results calculated for each of the variants.
 *
 * This processor will take the output and create :VariantEffect nodes in the
 * graph database, and then take the results of the trans-ethnic analysis and
 * create :EffectAnalysis nodes.
 *
 * The source tables are read from:
 *
 *  s3://dig-analysis-data/out/varianteffects/<dataset>/<phenotype>/effects
 */
class UploadEffectProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

  /**
   * All the processors this processor depends on.
   */
  override val dependencies: Seq[Processor.Name] = Seq(
    VariantEffectPipeline.variantEffectProcessor,
  )

  /**
   * All the job scripts that need to be uploaded to AWS.
   */
  override val resources: Seq[String] = Nil

  /**
   * Take all the phenotype results from the dependencies and process them.
   */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val datasets = results.map(_.output).distinct

    val ios = for (dataset <- datasets) yield {
      val analysis = new Analysis(s"VEP", Provenance.thisBuild)
      val graph    = new GraphDb(config.neo4j)

      // where the output is located
      val effects            = s"out/varianteffect/effects"
      val regulatoryFeatures = s"$effects/regulatory_feature_consequences"
      val transcripts        = s"$effects/transcript_consequences"

      for {
        _ <- IO(logger.info(s"Preparing upload of variant effects..."))

        // delete the existing analysis and recreate it
        id <- analysis.create(graph)

        // find all the part files to upload for the analysis
        _ <- IO(logger.info(s"Uploading regulatory feature consequences..."))
        _ <- analysis.uploadParts(aws, graph, id, regulatoryFeatures)(uploadRegulatoryFeatures)

        // find all the part files to upload for the analysis
        //_ <- IO(logger.info(s"Uploading transcript consequences..."))
        //_ <- analysis.uploadParts(aws, graph, id, transcripts)(uploadTranscripts)

        // add the result to the database
        _ <- Run.insert(pool, name, datasets, analysis.name)
        _ <- IO(logger.info("Done"))
      } yield ()
    }

    // process each phenotype serially
    ios.toList.sequence >> IO.unit
  }

  /**
   * Given a part file, upload it and create all the regulatory feature nodes.
   */
  def uploadRegulatoryFeatures(graph: GraphDb, id: Int, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |
                |// die if the variant doesn't exist
                |MATCH (v:Variant {name: r.id})
                |
                |// create the RegulatoryFeature analysis node
                |MERGE (n:RegulatoryFeature)-[:FOR_VARIANT]->(v)
                |ON CREATE SET
                |  biotype=r.biotype,
                |  consequenceTerms=split(r.consequence_terms, ','),
                |  impact=r.impact,
                |  featureId=r.regulatory_feature_id,
                |  pick=toInteger(r.pick) = 1
                |
                |// create the relationship to the analysis node
                |MERGE (q)<-[:PRODUCED_BY]-(n)
                |
                |// create the relationship to the variant
                |MERGE (v)<-[:FOR_VARIANT]-(n)
                |""".stripMargin

    graph.run(q)
  }

  /**
   * Given a part file, upload it and create all the transcript nodes.
   */
  def uploadTranscripts(graph: GraphDb, id: Int, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |
                |// die if the variant doesn't exist
                |MATCH (v:Variant {name: r.id})
                |
                |""".stripMargin

    graph.run(q)
  }
}
