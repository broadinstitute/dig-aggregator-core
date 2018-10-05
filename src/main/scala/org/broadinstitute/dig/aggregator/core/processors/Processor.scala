package org.broadinstitute.dig.aggregator.core.processors

import cats._
import cats.effect._

import doobie._

import com.typesafe.scalalogging.LazyLogging

import org.broadinstitute.dig.aggregator.core.config.BaseConfig

import org.rogach.scallop._

/**
 * A Processor is a unique. Flags and config are passed in, but not used here
 * because there needs to be type-safety assurances that all processors take
 * them as arguments when constructed dynamically.
 */
abstract class Processor(flags: Processor.Flags, config: BaseConfig) extends LazyLogging {

  /**
   * A unique name for this processor. Must be unique across all processors!
   */
  val name: Processor.Name

  /**
   * Run this processor.
   */
  def run(): IO[Unit]
}

/**
 * Companion object for registering the names of processors.
 */
object Processor {
  import scala.language.implicitConversions

  /**
   * A mapping of all the registered application names.
   */
  private var names: Map[String, (Flags, BaseConfig) => Processor] = Map()

  /**
   * Lookup a processor by name.
   */
  def apply(name: String): (Flags, BaseConfig) => Processor = {
    names(name)
  }

  /**
   * All processors are required to have a unique name that is unique across
   * every processor for use in the MySQL database to show what has been
   * processed already.
   */
  final class Name(name: String, ctor: (Flags, BaseConfig) => Processor) {

    /**
     * Get the name of this processor as a string.
     */
    override def toString: String = name

    /**
     * Registers the application name with the global map.
     */
    names.get(name) match {
      case Some(c) => require(c == ctor, s"$name already registered to another constructor")
      case None    => names += name -> ctor
    }
  }

  /**
   * Conversion from DB string to Processor.Name for doobie.
   */
  implicit val NameMeta: Meta[Name] = {
    Meta[String].xmap(name => new Name(name, names(name)), _.toString)
  }

  /**
   * All processors expect these options on the command line.
   */
  trait Flags { self: ScallopConf =>

    /** Force processor to reprocess data it already has processed. */
    val reprocess: ScallopOption[Boolean] = opt("reprocess")

    /** Actually run the processor (as opposed to just showing work). */
    val yes: ScallopOption[Boolean] = opt("yes")

    /** Only run the only processor and not all downstream processors. */
    val only: ScallopOption[Boolean] = opt("only")
  }
}
