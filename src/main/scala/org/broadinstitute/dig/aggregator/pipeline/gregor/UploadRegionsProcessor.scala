package org.broadinstitute.dig.aggregator.pipeline.gregor

import java.net.URL
import java.net.URLDecoder

import cats.effect.IO
import org.broadinstitute.dig.aggregator.core.{Analysis, GraphDb, Provenance, Run}
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors.{Processor, RunProcessor}
import org.broadinstitute.dig.aggregator.pipeline.metaanalysis.MetaAnalysisPipeline
import org.neo4j.driver.v1.StatementResult

class UploadRegionsProcessor(name: Processor.Name, config: BaseConfig) extends RunProcessor(name, config) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    GregorPipeline.sortRegionsProcessor,
    MetaAnalysisPipeline.metaAnalysisProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Nil

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def processResults(results: Seq[Run.Result]): IO[Unit] = {
    val graph    = new GraphDb(config.neo4j)
    val analysis = new Analysis(s"ChromatinState/Regions", Provenance.thisBuild)

    val io = for {
      //id <- analysis.create(graph)

      // find and upload all the sorted region part files
      //_ <- analysis.uploadParts(aws, graph, id, "out/gregor/regions/chromatin_state/")(uploadRegions)

      // connect all the variants to the regions that were uploaded
      _ <- IO(logger.info(s"Connecting variants to regions..."))
      _ <- connectVariants(graph)

      // add the result to the database
      _ <- Run.insert(pool, name, results.map(_.output).distinct, analysis.name)
      _ <- IO(logger.info("Done"))
    } yield ()

    // ensure that the graph connection is closed
    io.guarantee(graph.shutdown())
  }

  /** Create all the region nodes.
    */
  def uploadRegions(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val pattern = raw".*/biosample=([^/]+)/name=([^/]+)/.*".r
    val path    = URLDecoder.decode(new URL(part).getPath, "UTF-8")

    // Extract the tissue ID and annotation name from the HDFS file location.
    //
    // NOTE: When Spark ran on on the tissues, to prevent odd characters, we replaced
    //       ':' with '_', and here we'll put it back.
    val (tissue, name) = path match {
      case pattern(sampleId, annotation) =>
        sampleId.replaceFirst("_", ":") -> annotation
    }

    val q = s"""|USING PERIODIC COMMIT 50000
                |LOAD CSV FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |MATCH (t:Tissue {name: '$tissue'})
                |
                |// create an annotation for this region
                |MERGE (a:Annotation {name: '$name'})
                |ON CREATE SET
                |  a.type = 'ChromatinState'
                |
                |// create the result node (note the second label!)
                |CREATE (n:Region:RegionToBeProcessed {
                |  chromosome: r[0],
                |  start: toInteger(r[1]),
                |  end: toInteger(r[2])
                |})
                |
                |// create the relationships
                |CREATE (q)-[:PRODUCED]->(n)
                |CREATE (t)-[:HAS_REGION]->(n)
                |CREATE (n)-[:HAS_ANNOTATION]->(a)
                |""".stripMargin

    graph.run(q)
  }

  def connectVariants(graph: GraphDb): IO[StatementResult] = {
    val q = s"""|MATCH (r:RegionToBeProcessed) WITH r LIMIT {limit}
                |
                |// drop the label as this region has now been processed
                |REMOVE r:RegionToBeProcessed
                |WITH r
                |
                |// find all variants within this region
                |MATCH (v:Variant)
                |WHERE v.chromosome = r.chromosome
                |AND v.position >= r.start
                |AND v.position < r.end
                |
                |// connect the transcript consequence to the gene
                |MERGE (v)-[:HAS_REGION]->(r)
                |RETURN count(r)
                |""".stripMargin

    // run the query using the APOC function
    graph.run(s"call apoc.periodic.commit('$q', {limit:1000})")
  }
}
