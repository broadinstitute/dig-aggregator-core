package org.broadinstitute.dig.aggregator.core

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import doobie._
import java.util.UUID

import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aws.{AWS, JobStep}
import org.broadinstitute.dig.aws.emr.{Cluster, InstanceType, Spark}

/** Each processor has a globally unique name and a run function. */
abstract class Processor(val name: Processor.Name,
                         config: BaseConfig,
                         /** Database transactor for loading state, etc. */
                         protected val pool: DbPool)
    extends LazyLogging {

  /** The collection of resources this processor needs to have uploaded
    * before the processor can run.
    *
    * These resources are from the classpath and are uploaded to a parallel
    * location in HDFS so they may be referenced by scripts used to run
    * the processor on a cloud cluster.
    */
  val resources: Seq[String] = Seq.empty

  /** All the processors this processor depends on. Root processors should
    * depend on one (or more) of the processors in the IntakePipeline.
    */
  val dependencies: Seq[Processor.Name]

  /** The cluster definition to instantiate for this processor. This is a
    * very basic configuration that should work for a good number of jobs.
    */
  val cluster: Cluster = Cluster(
    name = name.toString,
    masterInstanceType = InstanceType.c5_4xlarge,
    slaveInstanceType = InstanceType.c5_2xlarge,
    instances = 5,
    masterVolumeSizeInGB = 100,
    slaveVolumeSizeInGB = 50,
    configurations = Seq(Spark.Env().withPython3)
  )

  /** AWS client for uploading resources and running jobs. */
  protected val aws: AWS = new AWS(config.aws)

  /** Given an input, determine what output(s) it should belong to.
    *
    * Every input should be represented in at least one output, but can be
    * present in multiple outputs. This method is used to build a map of
    * output -> seq[input.UUID], which is what's written to the database.
    */
  def getOutputs(input: Run.Result): Processor.OutputList

  /** Given an output, returns a sequence of job steps to be executed on
    * the cluster.
    */
  def getJob(output: String): Seq[JobStep]

  /** Process a set of run results. */
  def processOutputs(output: Seq[String]): IO[Unit] = {
    val jobs = output.map(getJob)

    // spins up the cluster(s) and runs all the jobs
    aws.runJobs(cluster, jobs)
  }

  /** Using all the outputs returned from `getOutputs`, build a map of
    * output -> seq[input], which will be written to the database.
    */
  def buildOutputMap(inputs: Seq[Run.Result], opts: Processor.Opts): Map[String, Set[UUID]] = {
    val inputToOutputs = inputs.map { input =>
      input -> getOutputs(input)
    }

    // get the list of output -> input UUID pairings
    val outputs = inputToOutputs.flatMap {
      case (input, Processor.Outputs(seq)) => seq.map(_ -> input.uuid)
      case _                               => Seq.empty
    }

    // group the inputs together by output name
    val outputMap = outputs.groupBy(_._1).mapValues(_.map(_._2))

    // find the unique list of inputs that should be in ALL outputs
    val inputUUIDsInAllOutputs = inputToOutputs
      .filter(_._2 == Processor.AllOutputs)
      .map(_._1.uuid)
      .distinct

    // append any inputs that belong to ALL outputs
    val finalMap = outputMap.mapValues { inputUUIDs =>
      (inputUUIDs ++ inputUUIDsInAllOutputs).toSet
    }

    // get all UUIDs across all outputs
    val allOutputInputUUIDs = finalMap.values.flatten.toSet

    // validate that ALL inputs are represented in at least one output
    inputs.map(_.uuid).foreach { uuid =>
      require(allOutputInputUUIDs.contains(uuid))
    }

    finalMap
      .filter { case (output, _) => opts.onlyGlobs.exists(_.matches(output)) }
      .filterNot { case (output, _) => opts.excludeGlobs.exists(_.matches(output)) }
  }

  /** Determines the set of things that need to be processed. */
  def getWork(opts: Processor.Opts): IO[Map[String, Set[UUID]]] = {
    for {
      inputs      <- Run.resultsOf(pool, dependencies)
      lastOutputs <- if (opts.reprocess) IO(Seq.empty) else Run.resultsOf(pool, name)
    } yield {
      val outputMap = buildOutputMap(inputs, opts).toList

      // for each output, filter the inputs that haven't been processed
      val filteredOutputs = outputMap.map {
        case (output, inputs) =>
          output -> inputs.filterNot { uuid =>
            lastOutputs.exists(r => {
              r.input match {
                case Some(inputUUID) => r.output == output && inputUUID == uuid
                case _               => false
              }
            })
          }
      }

      // remove any outputs with no inputs
      filteredOutputs.filterNot(_._2.isEmpty).toMap
    }
  }

  /** Complete all work and write to the database what was done. */
  def insertRuns(pool: DbPool, outputs: Map[String, Set[UUID]]): IO[Seq[UUID]] = {
    val insertRuns = for ((output, inputs) <- outputs.toList.sortBy(_._1)) yield {
      val inputUUIDs = inputs.toList.distinct

      // a run will be inserted for each unique input
      Run.insert(pool, name, output, NonEmptyList.fromListUnsafe(inputUUIDs))
    }

    for {
      _   <- IO(logger.info("Updating database..."))
      ids <- insertRuns.sequence
      _   <- IO(logger.info("Done"))
    } yield ids
  }

  /** Upload resources to S3. */
  def uploadResources(aws: AWS): IO[Unit] = {
    resources.map(aws.upload(_)).toList.sequence.as(())
  }

  /** True if this processor has something to process. */
  def hasWork(opts: Processor.Opts): IO[Boolean] = {
    getWork(opts).map(_.nonEmpty)
  }

  /** Logs the set of things this processor will process if run. */
  def showWork(opts: Processor.Opts): IO[Unit] = {
    for (work <- getWork(opts)) yield {
      if (work.isEmpty) {
        logger.info("Everything up to date.")
      } else {
        work.keys.foreach { output =>
          logger.info(s"Output $output will be built")
        }
      }
    }
  }

  /** Run this processor. */
  def run(opts: Processor.Opts): IO[Unit] = {
    getWork(opts).flatMap { work =>
      if (work.isEmpty) {
        IO(logger.info("Everything up to date."))
      } else {
        for {
          _ <- uploadResources(aws)

          // if only inserting runs, skip processing
          _ <- if (opts.insertRuns) IO.unit else processOutputs(work.keys.toSeq)
          _ <- if (opts.noInsertRuns) IO.unit else insertRuns(pool, work)
        } yield ()
      }
    }
  }
}

/** Companion object for registering the names of processors. */
object Processor extends LazyLogging {
  import scala.language.implicitConversions

  /** Process outputs. */
  sealed trait OutputList

  /** This input should be part of all outputs produced by the processor. */
  final case object AllOutputs extends OutputList

  /** Special case for intake processors. */
  final case object NoOutputs extends OutputList

  /** A single processor Output. */
  final case class Outputs(seq: Seq[String]) extends OutputList {
    require(seq.nonEmpty)
  }

  /** Command line flags processors know about. */
  final case class Opts(
      reprocess: Boolean,
      insertRuns: Boolean,
      noInsertRuns: Boolean,
      only: Option[String],
      exclude: Option[String],
  ) {
    import Glob.String2Glob

    /** Returns a glob for the only option. */
    lazy val onlyGlobs: Seq[Glob] = {
      only.map(_.split(",").map(_.toGlob).toSeq).getOrElse(List(Glob.True))
    }

    /** Returns a glob for the exclude option. */
    lazy val excludeGlobs: Seq[Glob] = {
      exclude.map(_.split(",").map(_.toGlob).toSeq).getOrElse(List(Glob.False))
    }
  }

  /** Processors are required to have a unique name that is unique across all
    * processors for use as keys in the MySQL database queries.
    *
    * Names cannot be created at any time. They must be created using with
    * the `register` function.
    */
  final class Name private[Processor] (private val name: String) {
    override def toString: String = name
    override def hashCode: Int    = name.hashCode
    override def equals(other: Any): Boolean = other match {
      case that: Name => this.name == that.name
      case _          => false
    }
  }

  /** Companion object. */
  object Name {

    /** Implicit conversion from/from DB string to Processor.Name for doobie. */
    implicit val nameGet: Get[Name] = Get[String].tmap(new Name(_))
    implicit val namePut: Put[Name] = Put[String].tcontramap(_.toString)
  }

  /** Every processor is constructed with a type-safe name and configuration. */
  type Constructor = (Processor.Name, BaseConfig, DbPool) => Processor

  /** A mapping of all the registered processor names. */
  private var names: Map[Processor.Name, Constructor] = Map()

  /** Register a processor name with a constructor. */
  def register(name: String, ctor: Constructor): Processor.Name = {
    val n = new Name(name)

    // ensure it does
    names.get(n) match {
      case Some(_) => throw new Exception(s"Processor '$name' already registered")
      case None    => names += n -> ctor; n
    }
  }

  /** Version of apply() that takes the actual process name. */
  def apply(name: Name): (BaseConfig, DbPool) => Option[Processor] = {
    val ctor = names.get(name)

    // lambda that will create this processor with a configuration
    { (config: BaseConfig, dbPool: DbPool) =>
      ctor.map(_(name, config, dbPool))
    }
  }

  /** Create a processor given its name and a configuration. */
  def apply(name: String): (BaseConfig, DbPool) => Option[Processor] = {
    apply(new Name(name))
  }
}
