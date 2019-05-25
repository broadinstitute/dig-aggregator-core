package org.broadinstitute.dig.aggregator.pipeline.gregor

import cats.effect._
import cats.implicits._
import java.net.URL
import java.net.URLDecoder

import org.broadinstitute.dig.aggregator.core.{AWS, Analysis, GraphDb, Provenance, Run}
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.{Processor, RunProcessor}
import org.neo4j.driver.v1.StatementResult

class UploadGlobalEnrichmentProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    GregorPipeline.globalEnrichmentProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Nil

  /** Each input is a phenotype, and the output is the analysis node.
    */
  override def getRunOutputs(results: Seq[Run.Result]): Map[String, Seq[String]] = {
    results
      .map(_.output)
      .distinct
      .map { phenotype =>
        s"GlobalEnrichment/$phenotype" -> Seq(phenotype)
      }
      .toMap
  }

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val graph = new GraphDb(config.neo4j)

    // each phenotype is uploaded individually
    val ios = for ((output, Seq(phenotype)) <- getRunOutputs(results)) yield {
      val analysis = new Analysis(output, Provenance.thisBuild)
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
                |MATCH (k:Annotation {name: annotation})
                |
                |// skip any enrichment with no p-value
                |WHERE NOT toFloat(r.PValue) IS NULL
                |
                |// create the result node
                |CREATE (n:GlobalEnrichment {
                |  inBedIndexSNP: toFloat(r.InBed_Index_SNP),
                |  expectedInBedIndexSNP: toFloat(r.ExpectNum_of_InBed_SNP),
                |  pValue: toFloat(r.PValue)
                |})
                |
                |// create the relationships
                |CREATE (q)-[:PRODUCED]->(n)
                |CREATE (n)-[:HAS_ANNOTATION]->(k)
                |CREATE (p)-[:HAS_GLOBAL_ENRICHMENT]->(n)
                |CREATE (a)-[:HAS_GLOBAL_ENRICHMENT]->(n)
                |CREATE (t)-[:HAS_GLOBAL_ENRICHMENT]->(n)
                |""".stripMargin

    graph.run(q)
  }
}
