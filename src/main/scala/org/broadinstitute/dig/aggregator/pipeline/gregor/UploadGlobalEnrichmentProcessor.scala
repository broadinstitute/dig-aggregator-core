package org.broadinstitute.dig.aggregator.pipeline.gregor

import java.net.URL
import java.net.URLDecoder

import org.broadinstitute.dig.aggregator.core.Analysis
import org.broadinstitute.dig.aggregator.core.GraphDb
import org.broadinstitute.dig.aggregator.core.Processor
import org.broadinstitute.dig.aggregator.core.Provenance
import org.broadinstitute.dig.aggregator.core.Run
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.neo4j.driver.v1.StatementResult

import cats.effect._
import cats.implicits._

class UploadGlobalEnrichmentProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    GregorPipeline.globalEnrichmentProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Nil

  /** Each phenotype is uploaded separately.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq(input.output))
  }

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val graph = new GraphDb(config.neo4j)

    // each phenotype is uploaded individually
    val ios = for (phenotype <- outputs) yield {
      val analysis = new Analysis(s"GlobalEnrichment/$phenotype", Provenance.thisBuild)
      val parts    = s"out/gregor/summary/$phenotype/"

      for {
        id <- analysis.create(graph)
        _  <- analysis.uploadParts(aws, graph, id, parts, ".txt")(uploadStatistics)
      } yield ()
    }

    // ensure that the graph connection is closed
    ios.toList.sequence.as(()).guarantee(graph.shutdown())
  }

  /** Create all the region nodes.
    */
  def uploadStatistics(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val pattern = raw".*/([^/]+)/([^/]+)/statistics.txt".r
    val path    = URLDecoder.decode(new URL(part).getPath, "UTF-8")

    // Extract the phenotype and ancestry name from the HDFS file location.
    val (phenotype, ancestry) = path match {
      case pattern(p, a) => p -> a
    }

    val q = s"""|USING PERIODIC COMMIT
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node, phenotype, and ancestry
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |MATCH (p:Phenotype {name: '$phenotype'})
                |MATCH (a:Ancestry {name: '$ancestry'})
                |
                |// split the "bed file" into tissue and annotation
                |WITH q, r, p, a, split(r.Bed_File, '___') AS bed
                |
                |// NOTE: When Spark ran on on the tissues, to prevent odd characters, we
                |//       replaced ':' with '_', and here we'll put it back.
                |
                |WITH q, r, p, a, replace(bed[0], '_', ':') AS tissue, bed[1] AS annotation
                |
                |// find the tissue
                |MATCH (t:Tissue {name: tissue})
                |
                |// skip any enrichment with no p-value
                |WHERE NOT toFloat(r.PValue) IS NULL
                |
                |// create the result node
                |CREATE (n:GlobalEnrichment {
                |  annotation: annotation,
                |  inBedIndexSNP: toFloat(r.InBed_Index_SNP),
                |  expectedInBedIndexSNP: toFloat(r.ExpectNum_of_InBed_SNP),
                |  pValue: toFloat(r.PValue)
                |})
                |
                |// create the relationships
                |CREATE (q)-[:PRODUCED]->(n)
                |CREATE (p)-[:HAS_GLOBAL_ENRICHMENT]->(n)
                |CREATE (a)-[:HAS_GLOBAL_ENRICHMENT]->(n)
                |CREATE (t)-[:HAS_GLOBAL_ENRICHMENT]->(n)
                |""".stripMargin

    graph.run(q)
  }
}
