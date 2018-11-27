package org.broadinstitute.dig.aggregator.core.processors

import cats._
import cats.effect._

import doobie._

import com.typesafe.scalalogging.LazyLogging

import org.broadinstitute.dig.aggregator.core.config.BaseConfig

import org.rogach.scallop._

/**
 * Each processor has a globally unique name and a run function.
 */
abstract class Processor(val name: Processor.Name) extends LazyLogging {

  /**
   * Determines the set of things that need to be processed.
   */
  def getWork(reprocess: Boolean, only: Option[String]): IO[Seq[_]]

  /**
   * True if this processor has something to process.
   */
  def hasWork(reprocess: Boolean): IO[Boolean] = {
    getWork(reprocess, None).map(_.nonEmpty)
  }

  /**
   * Logs the set of things this processor will process if run.
   */
  def showWork(reprocess: Boolean, only: Option[String]): IO[Unit] = {
    for (work <- getWork(reprocess, only)) yield {
      if (work.isEmpty) {
        logger.info(s"Everything up to date.")
      } else {
        work.foreach(i => logger.info(s"$i needs processed."))
      }
    }
  }

  /**
   * Run this processor.
   */
  def run(reprocess: Boolean, only: Option[String]): IO[Unit]
}

/**
 * Companion object for registering the names of processors.
 */
object Processor extends LazyLogging {
  import scala.language.implicitConversions

  /**
   * Processors are required to have a unique name that is unique across all
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

  /**
   * Companion object.
   */
  object Name {

    /**
     * Implicit conversion from/from DB string to Processor.Name for doobie.
     */
    implicit val nameGet: Get[Name] = Get[String].tmap(new Name(_))
    implicit val namePut: Put[Name] = Put[String].tcontramap(_.toString)
  }

  /**
   * Every processor is constructed with a type-safe name and configuration.
   */
  type Constructor = (Processor.Name, BaseConfig) => Processor

  /**
   * A mapping of all the registered processor names.
   */
  private var names: Map[Processor.Name, Constructor] = Map()

  /**
   * Register a processor name with a constructor.
   */
  def register(name: String, ctor: Constructor): Processor.Name = {
    val n = new Name(name)

    // ensure it does
    names.get(n) match {
      case Some(_) => throw new Exception(s"Processor '$name' already registered")
      case None    => names += n -> ctor; n
    }
  }

  /**
   * Version of apply() that takes the actual process name.
   */
  def apply(name: Name): BaseConfig => Option[Processor] = {
    val ctor = names.get(name)

    // lambda that will create this processor with a configuration
    { config: BaseConfig =>
      ctor.map(_(name, config))
    }
  }

  /**
   * Create a processor given its name and a configuration.
   */
  def apply(name: String): BaseConfig => Option[Processor] = {
    apply(new Name(name))
  }
}
