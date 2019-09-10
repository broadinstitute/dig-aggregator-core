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
  private val analysisName: String = "Gregor/AnnotatedRegions"

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
    val s3path   = "out/gregor/regions/"

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
    val pattern = raw".+/biosample(?:=|%3[dD])([^/]+)/method(?:=|%3[dD])([^/]+)/annotation(?:=|%3[dD])([^/]+)/.*".r

    // parse the part file to get extra components
    val (tissue, annotation) = part match {
      case pattern(biosample, _, name) => (biosample, name)
    }

    // build the query
    val q = s"""|USING PERIODIC COMMIT 10000
                |LOAD CSV WITH HEADERS FROM '$part' AS r
                |FIELDTERMINATOR '\t'
                |
                |// lookup the analysis node
                |MATCH (q:Analysis) WHERE ID(q)=$id
                |
                |// lookup the tissue and overlapped region
                |MATCH (t:Tissue {name: $tissue})
                |
                |// create the region node
                |CREATE (n:Region {
                |  name: r.name,
                |  chromosome: r.chromosome,
                |  start: toInteger(r.start),
                |  end: toInteger(r.end),
                |  annotation: $annotation,
                |  rgb: r.itemRgb
                |})
                |
                |// create the required relationships
                |CREATE (q)-[:PRODUCED]->(n)
                |CREATE (t)-[:HAS_REGION]->(n)
                |""".stripMargin

    graph.run(q)
  }
}
