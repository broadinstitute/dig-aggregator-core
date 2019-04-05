package org.broadinstitute.dig.aggregator.pipeline.varianteffect

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors._

import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.Session
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
 *  s3://dig-analysis-data/out/varianteffects/effects/regulatory_feature_consequences
 *
 * and:
 *
 *  s3://dig-analysis-data/out/varianteffects/effects/transcript_consequences
 */
class UploadVariantCQSProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

  /**
   * All the processors this processor depends on.
   */
  override val dependencies: Seq[Processor.Name] = Seq(
    VariantEffectPipeline.loadVariantCQSProcessor
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
    val graph    = new GraphDb(config.neo4j)

    val ios = for (dataset <- datasets) yield {
      val analysis = new Analysis(s"VEP", Provenance.thisBuild)

      // where the output is located
      val regulatoryFeatures = s"out/varianteffect/regulatory_feature_consequences"
      val transcripts        = s"out/varianteffect/transcript_consequences"

      for {
        id <- analysis.create(graph)

        // find all the part files to upload for the analysis
        _ <- analysis.uploadParts(aws, graph, id, regulatoryFeatures)(uploadRegulatoryFeatures)
        _ <- analysis.uploadParts(aws, graph, id, transcripts)(uploadTranscripts)

        // connect transcript consequences to genes
        _ <- IO(logger.info(s"Connecting transcript consequences to genes..."))
        _ <- connectGenes(graph)

        // TODO: connect transcriptId and regulatoryFeatureId as well

        // add the result to the database
        _ <- Run.insert(pool, name, datasets, analysis.name)
        _ <- IO(logger.info("Done"))
      } yield ()
    }

    // process each phenotype serially
    (ios.toList.sequence >> IO.unit).guarantee(graph.shutdown)
  }

  /**
   * Given a part file, upload it and create all the regulatory feature nodes.
   */
  def uploadRegulatoryFeatures(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 10000
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |MATCH (v:Variant {name: r.id})
                |
                |// create the feature result node
                |CREATE (n:RegulatoryFeature {
                |  biotype: r.biotype,
                |  consequenceTerms: split(r.consequence_terms, ','),
                |  impact: r.impact,
                |  pick: toInteger(r.pick) = 1,
                |  regulatoryFeatureId: r.regulatory_feature_id
                |})
                |
                |// create the relationship to the analysis node
                |MERGE (q)-[:PRODUCED]->(n)
                |
                |// create the relationship to the variant
                |MERGE (v)-[:HAS_REGULATORY_FEATURE]->(n)
                |""".stripMargin

    for {
      _      <- mergeVariants(graph, part)
      result <- graph.run(q)
    } yield result
  }

  /**
   * Given a part file, upload it and create all the transcript nodes.
   */
  def uploadTranscripts(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 10000
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |MATCH (v:Variant {name: r.id})
                |
                |// create the consequence result node
                |CREATE (n:TranscriptConsequence {
                |  aminoAcids: r.amino_acids,
                |  biotype: r.biotype,
                |  caddPhred: toFloat(r.cadd_phred),
                |  caddRaw: toFloat(r.cadd_raw),
                |  caddRawRankscore: toFloat(r.cadd_raw_rankscore),
                |  canonical: toInteger(r.canonical) = 1,
                |  ccds: r.ccds,
                |  cdnaEnd: toInteger(r.cdna_end),
                |  cdnaStart: toInteger(r.cdna_start),
                |  cdsEnd: toInteger(r.cds_end),
                |  cdsStart: toInteger(r.cds_start),
                |  clinvarClnsig: r.clinvar_clnsig,
                |  clinvarGoldenStars: r.clinvar_golden_stars,
                |  clinvarRs: r.clinvar_rs,
                |  clinvarTrait: r.clinvar_trait,
                |  codons: r.codons,
                |  consequenceTerms: split(r.consequence_terms, ','),
                |  dannRankscore: toFloat(r.dann_rankscore),
                |  dannScore: toFloat(r.dann_score),
                |  distance: toInteger(r.distance),
                |  eigenPcRaw: toFloat(r['eigen-pc-raw']),
                |  eigenPcRawRankscore: toFloat(r['eigen-pc-raw_rankscore']),
                |  eigenPhred: toFloat(r['eigen-phred']),
                |  eigenRaw: toFloat(r['eigen-raw']),
                |  fathmmConvertedRankscore: toFloat(r.fathmm_converted_rankscore),
                |  fathmmPred: split(r.fathmm_pred, ','),
                |  fathmmScore: split(r.fatmm_score, ','),
                |  fathmmMklCodingGroup: r['fathmm-mkl_coding_group'],
                |  fathmmMklCodingPred: r['fathmm-mkl_coding_group'],
                |  fathmmMklCodingRankscore: toFloat(r['fathmm-mkl_coding_group']),
                |  fathmmMklCodingScore: toFloat(r['fathmm-mkl_coding_group']),
                |  flags: split(r.flags, ','),
                |  geneId: r.gene_id,
                |  genocanyonScore: toFloat(r.genocanyon_score),
                |  genocanyonScoreRankscore: toFloat(r.genocanyon_score_rankscore),
                |  gerpPlusPlusNr: toFloat(r['gerp++_nr']),
                |  gerpPlusPlusRs: toFloat(r['gerp++_rs']),
                |  gerpRsRankscore: toFloat(r['gerp++_rs_rankscore']),
                |  gm12878ConfidenceValue: toFloat(r.gm12878_confidence_value),
                |  gm12878FitconsScore: toFloat(r.gm12878_fitcons_score),
                |  gm12878FitconsScoreRankscore: toFloat(r.gm12878_fitcons_score_rankscore),
                |  gtexV6pGene: r.gtex_v6p_gene,
                |  gtexV6pTissue: r.gtex_v6p_tissue,
                |  h1HescConfidenceValue: toFloat(r.hesc_confidence_value),
                |  h1HescFitconsScore: toFloat(r.hesc_fitcons_score),
                |  h1HescFitconsScoreRankscore: toFloat(r.hesc_fitcons_score_rankscore),
                |  huvecConfidenceValue: toFloat(r.huvec_confidence_value),
                |  huvecFitconsScore: toFloat(r.huvec_fitcons_score),
                |  huvecFitconsScoreRankscore: toFloat(r.huvec_fitcons_score_rankscore),
                |  impact: r.impact,
                |  integratedConfidenceValue: toFloat(r.integrated_confidence_value),
                |  integratedFitconsScore: toFloat(r.integrated_fitcons_score),
                |  integratedFitconsScoreRankscore: toFloat(r.integrated_fitcons_score_rankscore),
                |  interproDomain: r.interpro_domain,
                |  lof: r.lof,
                |  lofFilter: r.lof_filter,
                |  lofFlags: split(r.lof_flags, ','),
                |  lofInfo: r.lof_info,
                |  lrtConvertedRankscore: toFloat(r.lrt_converted_rankscore),
                |  lrtOmega: toFloat(r.lrt_omega),
                |  lrtPred: r.lrt_pred,
                |  lrtScore: toFloat(r.lrt_score),
                |  metalrPred: r.metalr_pred,
                |  metalrRankscore: toFloat(r.metalr_rankscore),
                |  metalrScore: toFloat(r.metalr_score),
                |  metasvmPred: r.metasvm_pred,
                |  metasvmRankscore: toFloat(r.metasvm_rankscore),
                |  metasvmScore: toFloat(r.metasvm_score),
                |  mutationAssessorPred: r.mutationassessor_pred,
                |  mutationAssessorScore: toFloat(r.mutationassessor_score),
                |  mutationAssessorScoreRankscore: toFloat(r.mutationassessor_score_rankscore),
                |  mutationAssessorUniprotId: r.mutationassessor_uniprotid,
                |  mutationAssessorVariant: r.mutationassessor_variant,
                |  mutationTasterAae: split(r.mutationtaster_aae, ','),
                |  mutationTasterConvertedRankscore: toFloat(r.mutationtaster_converted_rankscore),
                |  mutationTasterModel: split(r.mutationtaster_model, ','),
                |  mutationTasterPred: split(r.mutationtaster_pred, ','),
                |  mutationTasterScore: [x in split(r.mutationtaster_score, ',') where x <> '' | toFloat(x)],
                |  phastcons100WayVertebrate: toFloat(r.phastcons100way_vertebrate),
                |  phastcons100WayVertebrateRankscore: toFloat(r.phastcons100way_vertebrate_rankscore),
                |  phastcons20WayMammalian: toFloat(r.phastcons20way_mammalian),
                |  phastcons20WayMammalianRankscore: toFloat(r.phastcons20way_mammalian_rankscore),
                |  phylop100WayVertebrate: toFloat(r.phylop100way_vertebrate),
                |  phylop100WayVertebrateRankscore: toFloat(r.phylop100way_vertebrate_rankscore),
                |  phylop20WayMammalian: toFloat(r.phylop20way_mammalian),
                |  phylop20WayMammalianRankscore: toFloat(r.phylop20way_mammalian_rankscore),
                |  pick: toInteger(r.pick) = 1,
                |  polyphen2HdivPred: r.polyphen2_hdiv_pred,
                |  polyphen2HdivRankscore: toFloat(r.polyphen2_hdiv_rankscore),
                |  polyphen2HdivScore: toFloat(r.polyphen2_hdiv_score),
                |  polyphen2HvarPred: r.polyphen2_hvar_pred,
                |  polyphen2HvarRankscore: toFloat(r.polyphen2_hvar_rankscore),
                |  polyphen2HvarScore: toFloat(r.polyphen2_hvar_score),
                |  polyphenPrediction: r.polyphen_prediction,
                |  polyphenScore: toFloat(r.polyphen_score),
                |  proteinEnd: toInteger(r.protein_end),
                |  proteinStart: toInteger(r.protein_start),
                |  proveanConvertedRankscore: toFloat(r.provean_converted_rankscore),
                |  proveanPred: split(r.provean_pred, ','),
                |  proveanScore: split(r.provean_score, ','),
                |  reliabilityIndex: toInteger(r.reliability_index),
                |  siftConvertedRankscore: toFloat(r.sift_converted_rankscore),
                |  siftPred: split(r.sift_pred, ','),
                |  siftPrediction: r.sift_prediction,
                |  siftScore: split(r.sift_score, ','),
                |  siphy29WayLogodds: toFloat(r.siphy_29way_logodds),
                |  siphy29WayLogoddsRankscore: toFloat(r.siphy_29way_logodds_rankscore),
                |  siphy29WayPi: [x in split(r.siphy_29way_pi, ':') where x <> '' | toFloat(x)],
                |  strand: toInteger(r.strand),
                |  transcriptId: r.transcript_id,
                |  transcriptIdVest3: split(r.transcript_id_vest3, ','),
                |  transcriptVarVest3: split(r.transcript_var_vest3, ','),
                |  vest3Rankscore: toFloat(r.vest3_rankscore),
                |  vest3Score: [x in split(r.vest3_score, ',') where x <> '' | toFloat(x)]
                |})
                |
                |// create the relationship to the analysis node
                |MERGE (q)-[:PRODUCED]->(n)
                |
                |// create the relationship to the variant
                |MERGE (v)-[:HAS_TRANSCRIPT_CONSEQUENCE]->(n)
                |""".stripMargin

    for {
      _      <- mergeVariants(graph, part)
      result <- graph.run(q)
    } yield result
  }

  /**
   * Read the part file and create any variants not present.
   */
  def mergeVariants(graph: GraphDb, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 50000
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// split the ID into chrom, pos, reference, allele (CPRA)
                |WITH r, split(r.id, ':') AS cpra
                |
                |// create the variant node if it doesn't exist
                |MERGE (v:Variant {name: r.id})
                |ON CREATE SET
                |  v.chromosome=cpra[0],
                |  v.position=toInteger(cpra[1]),
                |  v.reference=cpra[2],
                |  v.alt=cpra[3]
                |""".stripMargin

    graph.run(q)
  }

  /**
   * Connect all transcript consequences to existing genes.
   */
  def connectGenes(graph: GraphDb): IO[StatementResult] = {
    val q = s"""|MATCH (n:TranscriptConsequence),
                |      (g:Gene {ensemblId: n.geneId})
                |
                |// consequences referencing a gene, but has no connection
                |WHERE exists(n.geneId) AND NOT ((g)-[:HAS_TRANSCRIPT_CONSEQUENCE]->(n))
                |
                |// limit the size for each call (using APOC)
                |WITH n, g
                |LIMIT {limit}
                |
                |// connect the transcript consequence to the gene
                |MERGE (g)-[:HAS_TRANSCRIPT_CONSEQUENCE]->(n)
                |""".stripMargin

    // run the query using the APOC function
    graph.run(s"call apoc.periodic.commit('$q', {limit: 10000})")
  }
}
