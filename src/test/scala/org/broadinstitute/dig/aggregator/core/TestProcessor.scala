package org.broadinstitute.dig.aggregator.core

import cats.effect._
import java.util.UUID

import org.broadinstitute.dig.aggregator.core.config.{BaseConfig, MySQLConfig, Neo4jConfig, Settings}
import org.broadinstitute.dig.aws.config.emr.{EmrConfig, SubnetId}
import org.broadinstitute.dig.aws.config.{AWSConfig, S3Config}
import org.scalatest.FunSuite

import scala.io.Source

object TestProcessor {
  import Processor.{Name, register}

  // used for instantiating a dummy processor
  lazy val dummyConfig: BaseConfig = {
    val dummyPort   = 12345
    val mySQLConfig = MySQLConfig("dummy-host", dummyPort, "mysql", "username", "password")
    val neo4jConfig = Neo4jConfig("dummy-host", dummyPort, "username", "password")
    val s3Config    = S3Config("dummy-bucket")
    val emrConfig   = EmrConfig("ssh-key", SubnetId("subnet-dummy"))
    val awsConfig   = AWSConfig(s3Config, emrConfig)

    // build a dummy configuration
    BaseConfig(awsConfig, mySQLConfig, neo4jConfig)
  }

  /** Creates a new instance of a dummy processor.
    */
  def makeProcessor(dep: Name*)(processorName: Name, c: BaseConfig, pool: DbPool): Processor = {
    new Processor(processorName, c, pool) {

      /** No dependencies to upload. */
      override val dependencies: Seq[Processor.Name] = dep

      /** Generate an output that's the same name as the processor. */
      override def getOutputs(input: Run.Result): Processor.OutputList =
        Processor.Outputs(Seq(s"${name.toString}_output"))

      /** Nothing to do. */
      override def processOutputs(outputs: Seq[String]): IO[Unit] = IO.unit

      /** Does nothing, just here for the trait. */
      override def run(opts: Processor.Opts): IO[Unit] = IO.unit
    }
  }

  // create some test processors and the dependency chain
  val a: Name = register("A", makeProcessor())
  val b: Name = register("B", makeProcessor(a))
  val c: Name = register("C", makeProcessor(b))
  val d: Name = register("D", makeProcessor())
  val e: Name = register("E", makeProcessor(c, d))
}
