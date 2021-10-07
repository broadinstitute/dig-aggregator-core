package org.broadinstitute.dig.aggregator.core

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dig.aws.{Emr, S3}

import scala.collection.mutable
import scala.util.{Failure, Try}

/** A method is an object that is broken down into one or more stages and
  * run on data that has been loaded into S3, producing new data, which
  * may be the input to other methods.
  */
abstract class Method extends LazyLogging {

  /** The provenance data for this method. */
  val provenance: Option[Provenance] = None

  /** All the stages of this method. */
  val stages = new mutable.ListBuffer[Stage]()

  /** Add a new stage to the method. */
  def addStage[S <: Stage](stage: S): Unit = stages += stage

  /** Called immediately before method execution to instantiate stages. */
  def initStages(implicit context: Context): Unit

  /** Unique name of this method, which defaults to the name of the class.
    *
    * It must be unique across all methods and is (optionally) used to
    * determine the output location where stages in this method will output
    * their results.
    */
  def getName: String = getClass.getSimpleName.stripSuffix("$")

  /** Lookup a stage in this method by name. */
  def getStage(name: String): Stage = {
    stages.find(_.getName == name).getOrElse {
      throw new NoSuchElementException(name)
    }
  }

  /** Get all the stages or just those matching the --stage name.
    */
  def filterStages(opts: Opts): Seq[Stage] = {
    opts.stage.toOption match {
      case None => stages.result()
      case Some(name) =>
        stages.result().filter(_.getName == name) match {
          case Nil => throw new NoSuchElementException(name)
          case seq => seq
        }
    }
  }

  /** Test stages until one is found to have work, and show what will be
    * done with it.
    */
  def showWork(opts: Opts): Unit = {
    filterStages(opts) match {
      case Nil           => logger.warn(s"No stage(s) found in $getName")
      case stagesToCheck => stagesToCheck.exists(_.showWork(opts)); ()
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
      logger.warn(s"will be treated as new and updated; are you sure?")

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
    val result = Util.time(s"Running Method '${getName}'", logger.info(_)) {
      val opts: Opts = new Opts(args.toList)

      // create the execution context
      implicit val context: Context = new Context(this, Option(opts.config)) {
        override lazy val db: Db          = if (opts.test()) new Db() else new Db(opts.config.aws.rds.secret.get)
        override lazy val s3: S3.Bucket   = new S3.Bucket(opts.config.aws.s3.bucket)
        override lazy val emr: Emr.Runner = new Emr.Runner(opts.config.aws.emr, s3.bucket)
      }

      // execute the method
      val result = Try {
        logger.info(s"Initializing stages...")
        initStages(context)

        // ensure the runs table exists
        logger.info(s"Connecting to ${opts.config.aws.rds.instance}...")
        Runs.migrate()

        // verify the action and execute it
        confirmReprocess(opts) {
          if (opts.dryRun()) logger.warn("Dry run; no outputs will be built")
          if (opts.yes() && opts.test()) logger.warn("Test run; outputs will be built to test location")

          // run or just show work that would be run if --yes was provided
          if (opts.yes()) run(opts) else showWork(opts)
        }

        // finished successfully
        logger.info("Done")
      }

      // close connections to the database regardless
      try { result } finally { context.db.close() }
    }

    // output any error reported
    result match {
      case Failure(ex) => logger.error(s"${ex.getClass.getName}: ${ex.getMessage}"); ex.printStackTrace()
      case _           => ()
    }
  }
}
