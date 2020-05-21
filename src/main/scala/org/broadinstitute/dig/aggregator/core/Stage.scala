package org.broadinstitute.dig.aggregator.core

import java.net.URI

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging

import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{ClusterDef, InstanceType, Spark}

import scala.util.DynamicVariable

/** Each processor has a globally unique name and a run function. */
abstract class Stage extends LazyLogging {

  /** Tracks where all the resources were uploaded to so they can be used
    * when building a JobStep.
    */
  private var resourceMap: Map[String, URI] = Map.empty

  /** Unique name of this stage, which defaults to the name of the class.
    *
    * This name - combined with the method name - uniquely identifies the
    * outputs of the stage in the database.
    */
  def getName: String = getClass.getSimpleName.stripSuffix("$")

  /** Additional resource files that should always be uploaded, but may not
    * be referenced directly via a resourceURI call. E.g. resource A, which
    * is fetched via resourceURI references B, which is not. B still needs
    * to be uploaded, but won't be unless added to this list.
    */
  def additionalResources: Seq[String] = Seq.empty

  /** All the processors this processor depends on. Root processors should
    * depend on one (or more) of the processors in the IntakePipeline.
    */
  val dependencies: Seq[Run.Input.Source]

  /** The cluster definition to instantiate for this processor. This is a
    * very basic configuration that should work for a good number of jobs.
    */
  def cluster: ClusterDef = ClusterDef(
    name = getName,
    masterInstanceType = InstanceType.c5_4xlarge,
    slaveInstanceType = InstanceType.c5_2xlarge,
    instances = 5,
    masterVolumeSizeInGB = 100,
    slaveVolumeSizeInGB = 50,
    configurations = Seq(
      Spark.Env().withPython3,
      Spark.Config().withMaximizeResourceAllocation,
    )
  )

  /** Given an input S3 key, determine what output(s) it maps to.
    *
    * Every input should be represented in at least one output, but can be
    * present in multiple outputs. This method is used to build a map of
    * output -> seq[(key, eTag)], which is what's written to the database.
    */
  def getOutputs(key: Run.Input): Stage.Outputs

  /** Given an output, returns a sequence of job steps to be executed on
    * the cluster.
    */
  def getJob(output: String): Seq[JobStep]

  /** Get a cached resource file URI.
    *
    * If not already cached, this function will UPLOAD the given resource
    * to S3 and cache the URI to it.
    */
  def resourceURI(name: String): URI = {
    val cachedURI = resourceMap.getOrElse(name, Method.aws.value.upload(name))

    // keep the cache up to date
    resourceMap += name -> cachedURI
    cachedURI
  }

  /** Process a set of run results. */
  def processOutputs(output: Seq[String], opts: Opts): IO[Unit] = IO {
    val jobs = output.map(output => output -> getJob(output))

    if (jobs.nonEmpty) {
      val outPrefix = if (opts.test()) "test" else "out"
      val method    = Method.current.value.getName
      val stage     = getName

      // set the environment for each of the steps in each job
      for ((job, jobSteps) <- jobs) {
        val bucket = s"s3://${opts.config.aws.s3.bucket}"
        val prefix = s"$outPrefix/$method/$stage/$job"

        jobSteps.foreach { step =>
          step.setEnv("BUCKET", bucket) // e.g. s3://dig-analysis-data
          step.setEnv("METHOD", method) // e.g. MetaAnalysis
          step.setEnv("STAGE", stage)   // e.g. Plot
          step.setEnv("JOB", job)       // e.g. T2D
          step.setEnv("PREFIX", prefix) // e.g. out/MetaAnalysis/Plot/T2D
        }
      }

      // upload all additional resources before running
      additionalResources.foreach(resourceURI)

      // spins up the cluster(s) and runs all the job steps
      Method.aws.value.runJobs(cluster.copy(name = s"$method.$stage"), jobs.map(_._2))
    }
  }

  /** Using all the outputs returned from `getOutputs`, build a map of
    * output -> Set[(key, eTag)], which will be written to the database
    * after a successful stage run.
    */
  def buildOutputMap(inputs: Seq[Run.Input], opts: Opts): Map[String, Set[Run.Input]] = {
    val inputToOutputs = inputs.map { input =>
      input -> getOutputs(input)
    }

    // get the list of output -> input
    val outputs = inputToOutputs.flatMap {
      case (input, Stage.Outputs.Set(out @ _*)) => out.toList.map(_ -> input)
      case _                                    => Seq.empty
    }

    // group the inputs together by output name
    val outputMap = outputs.groupBy(_._1).view.mapValues(_.map(_._2))

    // find the unique list of inputs that should be in ALL outputs
    val inputsInAllOutputs = inputToOutputs
      .filter(_._2 == Stage.Outputs.All)
      .map(_._1)
      .distinct

    // append any inputs that belong to ALL outputs to each of them
    val finalMap = outputMap.mapValues { inputs =>
      (inputs ++ inputsInAllOutputs).toSet
    }

    // get all UUIDs across all outputs
    val allOutputInputs = finalMap.values.flatten.toSet

    // validate that ALL inputs are represented in at least one output
    inputs.foreach { input =>
      require(allOutputInputs.exists(_.key == input.key))
    }

    // filter by CLI options
    finalMap
      .filter { case (output, _) => opts.onlyGlobs.exists(_.matches(output)) }
      .filterNot { case (output, _) => opts.excludeGlobs.exists(_.matches(output)) }
      .toMap
  }

  /** Determines the set of things that need to be processed. This is
    * a mapping of output -> Set[key -> eTag].
    */
  def getWork(opts: Opts): IO[Map[String, Set[Run.Input]]] = {
    val getOutputs = if (opts.reprocess()) IO(Seq.empty) else Run.resultsOf(Method.pool.value, this)

    // get all the outputs that have already been processed
    for (lastOutputs <- getOutputs) yield {
      val inputs    = dependencies.flatMap(dep => dep.objects)
      val outputMap = buildOutputMap(inputs, opts)

      /* For every output that would run, remove all the inputs that have
       * already been processed by the stage for that output.
       *
       * NB: It's possible that the same input exists in multiple outputs,
       *     so don't try and simplify this without care!
       */

      val updatedOutputMap = outputMap.map {
        case (output, inputs) =>
          val results = lastOutputs.filter(_.output == output)

          // filter inputs that are already processed
          output -> inputs.filter { input =>
            results.find(_.input == input.key) match {
              case Some(result) if result.version == input.eTag => false
              case _                                            => true
            }
          }
      }

      // remove outputs from the map with no inputs
      updatedOutputMap.filter { case (_, inputs) => inputs.nonEmpty }
    }
  }

  /** Complete all work and write to the database what was done. */
  def insertRuns(outputs: Map[String, Set[Run.Input]]): IO[Unit] = {
    val insertRuns = for ((output, inputs) <- outputs.toList.sortBy(_._1)) yield {
      Run.insert(Method.pool.value, this, output, NonEmptyList.fromListUnsafe(inputs.toList))
    }

    for {
      _ <- IO(logger.info("Updating database..."))
      _ <- insertRuns.sequence
      _ <- IO(logger.info("Done"))
    } yield ()
  }

  /** True if this processor has new/updated dependencies. */
  def hasWork(opts: Opts): IO[Boolean] = {
    getWork(opts).map(_.nonEmpty)
  }

  /** Logs the set of things this processor will process if run. */
  def showWork(opts: Opts): IO[Unit] = {
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

  /** Run this stage. */
  def run(opts: Opts): IO[Unit] = {
    getWork(opts).flatMap { outputMap =>
      if (outputMap.isEmpty) {
        IO(logger.info("Everything up to date."))
      } else {
        for {
          _ <- if (opts.insertRuns()) IO.unit else processOutputs(outputMap.keys.toSeq, opts)
          _ <- if (opts.noInsertRuns()) IO.unit else insertRuns(outputMap)
        } yield ()
      }
    }
  }
}

/** Companion object for stage inputs and outputs. */
object Stage {

  /** Outputs are used by the stage to identify the job steps to execute.
    * They can be any any custom string desired by the stage.
    *
    * Example:
    *
    * A stage may use the path name of an input to parse out a phenotype
    * name. All inputs of the same phenotype will be processed together
    * as a single output, identified by the phenotype.
    */
  sealed trait Outputs

  /** */
  object Outputs {

    /** Special case to identify everything previously output. */
    final object All extends Outputs

    /** Hand-named outputs. */
    final case class Set(set: String*) extends Outputs
  }
}
