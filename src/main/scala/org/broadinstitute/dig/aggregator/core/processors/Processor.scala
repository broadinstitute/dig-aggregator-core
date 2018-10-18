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
   * Run this processor.
   */
  def run(flags: Processor.Flags): IO[Unit]
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
   * Implicit conversion from DB string to Processor.Name for doobie.
   */
  implicit val NameMeta: Meta[Name] = {
    Meta[String].xmap(new Name(_), _.toString)
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
   * Create a processor given its name and a configuration.
   */
  def apply(name: String): BaseConfig => Option[Processor] = {
    val n    = new Name(name)
    val ctor = names.get(n)

    // lambda that will create this processor with a configuration
    { config: BaseConfig =>
      ctor.map(_(n, config))
    }
  }

  /**
   * All processors expect these options on the command line.
   */
  trait Flags { self: ScallopConf =>

    /** Force processor to reprocess data it already has processed. */
    val reprocess: ScallopOption[Boolean] = opt("reprocess")

    /** Actually run the processor (as opposed to just showing work). */
    val yes: ScallopOption[Boolean] = opt("yes")
  }
}
