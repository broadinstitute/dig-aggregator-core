package org.broadinstitute.dig.aggregator.core

import com.typesafe.scalalogging.LazyLogging

import scala.util.Failure

/** A method is an object that is broken down into one or more stages and
  * run on data that has been loaded into S3, producing new data, which
  * may be the input to other methods.
  */
abstract class Method extends LazyLogging {

  /** All the stages of this method. */
  private var stages = List.empty[Stage]

  /** Called right at startup, before the method is actually run.
    *
    * Stages are added - in the order they should execute - using the
    * addStage function, from within here.
    */
  def initStages(): Unit

  /** The provenance data for this method. */
  val provenance: Option[Provenance] = None

  /** Unique name of this method, which defaults to the name of the class.
    *
    * It must be unique across all methods and is (optionally) used to
    * determine the output location where stages in this method will output
    * their results.
    */
  def getName: String = getClass.getSimpleName.stripSuffix("$")

  /** Add a new stage to the method. */
  def addStage(stage: Stage): Unit = {
    stages +:= stage
  }

  /** Lookup a stage in this method by name. */
  def getStage(name: String): Stage = {
    stages.find(_.getName == name).getOrElse {
      throw new Exception(s"No such stage $name in method.")
    }
  }

  /** Get all the stages or just those matching the --stage name.
    */
  def filterStages(opts: Opts): Seq[Stage] = {
    opts.stage.toOption match {
      case None => stages
      case Some(name) =>
        stages.filter(_.getName == name) match {
          case Nil    => throw new Exception(s"""No stages found in $getName matching "$name"""")
          case stages => stages
        }
    }
  }

  /** Test stages until one is found to have work, and show what will be
    * done with it.
    */
  def showWork(opts: Opts): Unit = {
    filterStages(opts) match {
      case Nil           => logger.warn(s"No stage(s) found in $getName")
      case stagesToCheck => stagesToCheck.foreach(_.showWork(opts))
    }
  }

  /** Run the entire method.
    */
  private def run(opts: Opts): Unit = {
    filterStages(opts) match {
      case Nil         => logger.warn(s"No stage(s) found in $getName")
      case stagesToRun => stagesToRun.foreach(_.run(opts))
    }
  }

  /** Checks if both --reprocess and --yes are present. If so, prompt the
    * user to be sure this is what they want to do before executing body.
    */
  private def confirmReprocess(opts: Opts)(body: => Unit): Unit = {
    import scala.io.StdIn

    def confirm(): Boolean = {
      val task = opts.stage.toOption.map(getStage).map(_.getName).getOrElse(getName)

      // use the stage name if --stage is provided, otherwise the method name
      logger.warn(s"The reprocess flag was passed. All inputs to $task")
      logger.warn("will be treated as new and updated; are you sure?")

      // only succeed if the user types in 'y'
      StdIn.readLine("[y/N]: ").equalsIgnoreCase("y")
    }

    // warn if needed, or just execute
    if (opts.dryRun() || !opts.reprocess() || confirm()) {
      body
    }
  }

  /** Entry point to the method.
    *
    * This is - essentially - the main() method for the JVM. Your method
    * instance will inherit this class and be specified as the entry point
    * for the JAR.
    */
  def main(args: Array[String]): Unit = {
    implicit val opts: Opts = new Opts(args.toList)

    // set implicits for the rest of the execution
    val result = Context.use(this) {
      logger.info(s"Connecting to ${opts.config.aws.rds.instance}")

      // ensure the runs table exists
      Runs.migrate()

      // add all the stages to the method
      initStages()

      // verify the action and execute it
      confirmReprocess(opts) {
        if (opts.dryRun()) logger.warn("Dry run; no outputs will be built")
        if (opts.yes() && opts.test()) logger.warn("Test run; outputs will be built to test location")

        // run or just show work that would be run if --yes was provided
        if (opts.yes()) run(opts) else showWork(opts)
      }

      logger.info("Done")
    }

    // output any error reported
    result match {
      case Failure(ex) => logger.error(ex.getMessage)
      case _           => ()
    }
  }
}
