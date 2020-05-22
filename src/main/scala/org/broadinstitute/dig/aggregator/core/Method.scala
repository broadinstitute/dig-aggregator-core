package org.broadinstitute.dig.aggregator.core

import com.typesafe.scalalogging.LazyLogging

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
  def getStage(name: String): Option[Stage] = {
    stages.find(_.getName == name)
  }

  /** Output all the work that each processor in the pipeline has to do. This
    * will only show a single level of work, and not later work that may need
    * to happen downstream.
    */
  def showWork(opts: Opts, stage: Option[String] = None): Unit = {
    val stagesToRun = stagesWithWork(opts)

    if (stagesToRun.isEmpty) {
      logger.info("Method output is up to date; nothing to do.")
    } else {
      stagesToRun.filter(s => stage.getOrElse(s) == s).foreach { stage =>
        logger.info(s"Stage ${stage.getName} will be run.")
      }
    }
  }

  /** Returns the list of stages that have new/updated dependencies. The
    * order of the stages returned is stable with respect to the order
    * they were added to the method.
    */
  private def stagesWithWork(opts: Opts): List[Stage] = {
    stages.filter { stage =>
      logger.info(s"Checking for new/updated inputs for ${stage.getName}...")
      stage.hasWork(opts)
    }
  }

  /** Execute a single stage of the method by name.
    */
  private def executeStage(name: String, opts: Opts): Unit = {
    getStage(name).map(_.run(opts)).get
  }

  /** Run the entire method.
    *
    * This function will determine which stages have new or updated
    * dependencies and then run them in the order they were added to
    * the method. Once those stages have completed, this function is
    * called again recursively. This repeats until there is no more
    * stages with work.
    */
  @scala.annotation.tailrec
  private def execute(opts: Opts): Unit = {
    val stagesToRun = stagesWithWork(opts)

    if (stagesToRun.isEmpty) {
      logger.info("Method output is up to date; nothing to do.")
    } else {
      stagesToRun.foreach(_.run(opts))

      // recurse until all stages are up-to-date
      execute(opts)
    }
  }

  /** Checks if both --reprocess and --yes are present. If so, prompt the
    * user to be sure this is what they want to do before executing body.
    */
  private def confirmReprocess(opts: Opts)(body: => Unit): Unit = {
    import scala.io.StdIn

    def warn(): Boolean = {
      logger.warn("The reprocess flag was passed. All stages")
      logger.warn("will be run; are you sure?")

      // only succeed if the user types in 'y'
      StdIn.readLine("[y/N]: ").equalsIgnoreCase("y")
    }

    // warn if needed, or just execute
    if (!opts.reprocess() || !opts.yes() || warn()) {
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
    Context.use(this) {
      logger.info(s"Connecting to ${opts.config.aws.rds.instance}")

      // ensure the runs table exists
      Runs.migrate()

      // add all the stages to the method
      initStages()

      // verify the action and execute it
      confirmReprocess(opts) {
        implicit val method: Method = this

        logger.info(s"Running $getName...")

        opts.stage.toOption match {
          case Some(name) => if (opts.yes()) executeStage(name, opts) else showWork(opts, Some(name))
          case _          => if (opts.yes()) execute(opts) else showWork(opts)
        }
      }
    }

    ()
  }
}
