package org.broadinstitute.dig.aggregator.core

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import com.typesafe.scalalogging.LazyLogging

import doobie._

import java.util.UUID

import org.broadinstitute.dig.aggregator.core.config.BaseConfig

/** Each processor has a globally unique name and a run function. */
abstract class Processor(val name: Processor.Name, config: BaseConfig) extends LazyLogging {

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

  /** Database transactor for loading state, etc. */
  protected val pool: DBPool = DBPool.fromMySQLConfig(config.mysql)

  /** AWS client for uploading resources and running jobs. */
  protected val aws: AWS = new AWS(config.aws)

  /** Process a set of run results. */
  def processResults(results: Seq[Run.Result]): IO[_]

  /** Given a set of work to do, return the Runs that should be written to
    * the database once that work has completed. The output of this function
    * should be a Map of Output name -> sequence of input run IDs that were
    * used to generate the output.
    *
    * If all results passed to this function are NOT represented in the
    * output map, then it is an error!
    */
  def getRunOutputs(work: Seq[Run.Result]): Map[String, Seq[UUID]]

  /** Determines the set of things that need to be processed. */
  def getWork(opts: Processor.Opts): IO[Seq[Run.Result]] = {
    for {
      results <- if (opts.reprocess) {
        Run.resultsOf(pool, dependencies)
      } else {
        Run.resultsOf(pool, dependencies, name)
      }
    } yield {
      results
        .filter(r => opts.onlyGlobs.exists(_.matches(r.output)))
        .filterNot(r => opts.excludeGlobs.exists(_.matches(r.output)))
    }
  }

  /** Complete all work and write to the database what was done. */
  def insertRuns(pool: DBPool, work: Seq[Run.Result]): IO[Seq[UUID]] = {
    val runOutputs = getRunOutputs(work)

    // validate that ALL run inputs are represented in the outputs
    val allOutputInputs = runOutputs.values.flatten.toSet
    val allInputs       = work.map(_.uuid).toSet

    // verify that allInputs are represented in allOutputInputs
    allInputs.foreach { input =>
      require(allOutputInputs contains input)
    }

    // build a list of all inserts, sorted by output
    val insertRuns = for ((output, inputs) <- runOutputs.toList.sortBy(_._1)) yield {
      Run.insert(pool, name, output, NonEmptyList.fromListUnsafe(inputs.toList))
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
        logger.info(s"Everything up to date.")
      } else {
        work.foreach(i => logger.info(s"$i needs processed."))
      }
    }
  }

  /** Run this processor. */
  def run(opts: Processor.Opts): IO[Unit] = {
    for {
      work <- getWork(opts)
      _    <- uploadResources(aws)

      // if only inserting runs, skip processing
      _ <- if (opts.insertRuns) IO.unit else processResults(work)
      _ <- insertRuns(pool, work)
    } yield ()
  }
}

/** Companion object for registering the names of processors. */
object Processor extends LazyLogging {
  import scala.language.implicitConversions

  /** Command line flags processors know about. */
  final case class Opts(reprocess: Boolean, insertRuns: Boolean, only: Option[String], exclude: Option[String]) {
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
  type Constructor = (Processor.Name, BaseConfig) => Processor

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
  def apply(name: Name): BaseConfig => Option[Processor] = {
    val ctor = names.get(name)

    // lambda that will create this processor with a configuration
    { config: BaseConfig =>
      ctor.map(_(name, config))
    }
  }

  /** Create a processor given its name and a configuration. */
  def apply(name: String): BaseConfig => Option[Processor] = {
    apply(new Name(name))
  }
}
