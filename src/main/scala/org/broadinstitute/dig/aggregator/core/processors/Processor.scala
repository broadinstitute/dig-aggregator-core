package org.broadinstitute.dig.aggregator.core.processors

import cats.effect._
import cats.implicits._
import doobie._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.{AWS, DbPool, Glob, Run}

/** Each processor has a globally unique name and a run function.
  */
abstract class Processor[T <: Run.Input](val name: Processor.Name) extends LazyLogging {

  /** The collection of resources this processor needs to have uploaded
    * before the processor can run.
    *
    * These resources are from the classpath and are uploaded to a parallel
    * location in HDFS!
    */
  val resources: Seq[String]

  /** Determines the set of things that need to be processed.
    */
  def getWork(opts: Processor.Opts): IO[Seq[T]]

  /** Given a set of work to do, return the Runs that should be written to
    * the database once that work has completed.
    */
  def getRunOutputs(work: Seq[T]): Map[String, Seq[String]]

  /** Complete all work and write to the database what was done.
    */
  def insertRuns(pool: DbPool, work: Seq[T]): IO[Seq[String]] = {
    val runOutputs = getRunOutputs(work)

    // validate that ALL run inputs are represented in the outputs
    val allOutputInputs = runOutputs.values.flatten.toSet
    val allInputs       = work.map(_.asRunInput).toSet

    // verify that allInputs are represented in allOutputInputs
    allInputs.foreach { input =>
      require(allOutputInputs contains input)
    }

    // build a list of all inserts, sorted by output
    val runs = for ((output, inputs) <- runOutputs.toList.sortBy(_._1)) yield {
      Run.insert(pool, name, output, inputs)
    }

    for {
      _   <- IO(logger.info("Updating database..."))
      ids <- runs.sequence
      _   <- IO(logger.info("Done"))
    } yield ids
  }

  /** Upload resources to S3.
    */
  def uploadResources(aws: AWS): IO[Unit] = {
    resources.map(aws.upload(_)).toList.sequence.as(())
  }

  /** True if this processor has something to process.
    */
  def hasWork(opts: Processor.Opts): IO[Boolean] = {
    getWork(opts).map(_.nonEmpty)
  }

  /** Logs the set of things this processor will process if run.
    */
  def showWork(opts: Processor.Opts): IO[Unit] = {
    for (work <- getWork(opts)) yield {
      if (work.isEmpty) {
        logger.info(s"Everything up to date.")
      } else {
        work.foreach(i => logger.info(s"$i needs processed."))
      }
    }
  }

  /** Run this processor.
    */
  def run(opts: Processor.Opts): IO[Unit]
}

/** Companion object for registering the names of processors.
  */
object Processor extends LazyLogging {
  import scala.language.implicitConversions

  /** Command line flags processors know about.
    */
  final case class Opts(reprocess: Boolean, insertRuns: Boolean, only: Option[String], exclude: Option[String]) {
    import Glob.String2Glob

    /** Returns a glob for the only option. */
    lazy val onlyGlobs: Seq[Glob] = only.map(_.split(",").map(_.toGlob).toSeq).getOrElse(List(Glob.True))

    /** Returns a glob for the exclude option. */
    lazy val excludeGlobs: Seq[Glob] = exclude.map(_.split(",").map(_.toGlob).toSeq).getOrElse(List(Glob.False))
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

  /** Companion object.
    */
  object Name {

    /** Implicit conversion from/from DB string to Processor.Name for doobie.
      */
    implicit val nameGet: Get[Name] = Get[String].tmap(new Name(_))
    implicit val namePut: Put[Name] = Put[String].tcontramap(_.toString)
  }

  /** Every processor is constructed with a type-safe name and configuration.
    */
  type Constructor = (Processor.Name, BaseConfig) => Processor[_ <: Run.Input]

  /** A mapping of all the registered processor names.
    */
  private var names: Map[Processor.Name, Constructor] = Map()

  /** Register a processor name with a constructor.
    */
  def register(name: String, ctor: Constructor): Processor.Name = {
    val n = new Name(name)

    // ensure it does
    names.get(n) match {
      case Some(_) => throw new Exception(s"Processor '$name' already registered")
      case None    => names += n -> ctor; n
    }
  }

  /** Version of apply() that takes the actual process name.
    */
  def apply(name: Name): BaseConfig => Option[Processor[_ <: Run.Input]] = {
    val ctor = names.get(name)

    // lambda that will create this processor with a configuration
    { config: BaseConfig =>
      ctor.map(_(name, config))
    }
  }

  /** Create a processor given its name and a configuration.
    */
  def apply(name: String): BaseConfig => Option[Processor[_ <: Run.Input]] = {
    apply(new Name(name))
  }
}
