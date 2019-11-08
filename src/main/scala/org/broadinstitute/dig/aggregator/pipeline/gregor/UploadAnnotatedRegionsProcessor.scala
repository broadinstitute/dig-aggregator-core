package org.broadinstitute.dig.aggregator.pipeline.gregor

import cats.effect._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core._
import org.neo4j.driver.v1.StatementResult

class UploadAnnotatedRegionsProcessor(name: Processor.Name, config: BaseConfig) extends Processor(name, config) {

  /** All the processors this processor depends on.
    */
  override val dependencies: Seq[Processor.Name] = Seq(
    GregorPipeline.sortRegionsProcessor,
  )

  /** All the job scripts that need to be uploaded to AWS.
    */
  override val resources: Seq[String] = Nil

  /** Name of the analysis node when uploading results.
    */
  private val analysisName: String = "GREGOR/AnnotatedRegions"

  /** All inputs are uploaded into a single output.
    */
  override def getOutputs(input: Run.Result): Processor.OutputList = {
    Processor.Outputs(Seq(analysisName))
  }

  /** Take all the phenotype results from the dependencies and process them.
    */
  override def processOutputs(outputs: Seq[String]): IO[Unit] = {
    val graph    = new GraphDb(config.neo4j)
    val analysis = new Analysis(analysisName, Provenance.thisBuild)
    val s3path   = "out/gregor/regions/unsorted/"

    val io = for {
      id <- analysis.create(graph)

      // find and upload all the sorted region part files
      _ <- analysis.uploadParts(aws, graph, id, s3path)(uploadRegions)
    } yield ()

    // ensure that the graph connection is closed
    io.guarantee(graph.shutdown())
  }

  /** Create all the region nodes.
    */
  def uploadRegions(graph: GraphDb, id: Long, part: String): IO[StatementResult] = {
    val q = s"""|USING PERIODIC COMMIT 10000
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// extract the tissue
                |WITH r, replace(r.biosample, '_', ':') AS tissue
                |
                |// lookup the analysis node and tissue
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |MATCH (t:Tissue {name: tissue})
                |
                |// join columns to make region name
                |WITH q, t, r, r.chromosome + ':' + r.start + '-' + r.end AS name
                |
                |// ensure the annotation node exists
                |MERGE (a:Annotation {name: r.annotation})
                |
                |// create the annotated region node
                |CREATE (n:AnnotatedRegion {
                |  name: name,
                |  chromosome: r.chromosome,
                |  start: toInteger(r.start),
                |  end: toInteger(r.end),
                |  rgb: r.itemRgb,
                |  score: toFloat(r.score)
                |})
                |
                |// create the required relationships
                |CREATE (q)-[:PRODUCED]->(n)
                |CREATE (n)-[:HAS_TISSUE]->(t)
                |CREATE (n)-[:HAS_ANNOTATION]->(a)
                |
                |// method is optional
                |FOREACH(i IN (CASE r.method WHEN null THEN [] WHEN 'NA' THEN [] ELSE [1] END) |
                |  MERGE (m:Method {name: r.method})
                |  CREATE (n)-[:HAS_METHOD]->(m)
                |)
                |""".stripMargin

    graph.run(q)
  }
}
