package org.broadinstitute.dig.aggregator.core

import java.io.File

import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.rogach.scallop.exceptions.ScallopException

/**
 * Command line and configuration file argument parsing.
 */
class Opts[C <: BaseConfig](args: Array[String])(implicit m: Manifest[C]) extends ScallopConf(args) {
  val configFile: ScallopOption[File] = opt("config", default = Some(new File("config.json")))

  /** Force Kafka consumption from the beginning of time. */
  val fromBeginning: ScallopOption[Boolean] = opt("from-beginning", required = false)

  /** Force Kafka consumption to continue from the state file. */
  val continue: ScallopOption[Boolean] = opt("continue", required = false)

  /** Show version info and quit. */
  val version: ScallopOption[Boolean] = opt("version", required = false)
  
  // ensure the configuration file exists if provided
  validateFileExists(configFile)

  // both --continue and --from-beginning cannot be specified
  mutuallyExclusive(continue, fromBeginning)
  
  mutuallyExclusive(version, continue)
  mutuallyExclusive(version, fromBeginning)

  verify
  
  /*
   * By default, Scallop will terminate the JVM on any ScallopExceptions, which is very bad for testing. Provide
   * New behavior, where ScallopExceptions still get thrown, but don't terminate the JVM.
   */
  override def onError(e: Throwable): Unit = e match {
    case e @ ScallopException(message) => {
      printHelp(message)
      
      throw e
    }
    case ex => super.onError(ex)
  }
  
  /* Private configuration settings. */
  lazy val config: C = configFile.toOption.map(Config.load[C]).get

  /** If --continue is not supplied, start from the beginning or end. */
  lazy val position: State.Position = {
    val from: Option[State.Position] = if(continue.isSupplied) Some(State.Continue) else None
    val beginning: Option[State.Position] = if(fromBeginning.isSupplied) Some(State.Beginning) else None

    (from orElse beginning).getOrElse(State.End)
  }
  
  private def printHelp(message: String): Unit = {
    printHelp()
    
    println()
    
    println(message)
  }
}
