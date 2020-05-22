package org.broadinstitute.dig.aggregator.core

import java.net.URI

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.{ClusterDef, InstanceType, Spark}

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
  val dependencies: Seq[Input.Source]

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

  /** Given an input object, determine what output(s) it maps to.
    *
    * Every input should be represented in at least one output, but can be
    * present in multiple outputs. This method is used to build a map of
    * output -> seq[(key, eTag)], which is what's written to the database.
    */
  def getOutputs(input: Input): Outputs

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
    val cachedURI = resourceMap.getOrElse(name, Context.current.aws.upload(name))

    // keep the cache up to date
    resourceMap += name -> cachedURI
    cachedURI
  }

  /** Process a set of run results. */
  def processOutputs(output: Seq[String], opts: Opts): Unit = {
    val jobs = output.map(output => output -> getJob(output))

    if (jobs.nonEmpty) {
      val outPrefix = if (opts.test()) "test" else "out"
      val method    = Context.current.method

      // set the environment for each of the steps in each job
      for ((job, jobSteps) <- jobs) {
        val bucket = s"s3://${opts.config.aws.s3.bucket}"
        val prefix = s"$outPrefix/${method.getName}/$getName/$job"

        jobSteps.foreach { step =>
          step.setEnv("BUCKET", bucket)         // e.g. s3://dig-analysis-data
          step.setEnv("METHOD", method.getName) // e.g. MetaAnalysis
          step.setEnv("STAGE", getName)         // e.g. Plot
          step.setEnv("JOB", job)               // e.g. T2D
          step.setEnv("PREFIX", prefix)         // e.g. out/MetaAnalysis/Plot/T2D
        }
      }

      // upload all additional resources before running
      additionalResources.foreach(resourceURI)

      // spins up the cluster(s) and runs all the job steps
      Context.current.aws.runJobs(cluster.copy(name = s"${method.getName}.$getName"), jobs.map(_._2))
    }
  }

  /** Using all the outputs returned from `getOutputs`, build a map of
    * output -> Set[(key, eTag)], which will be written to the database
    * after a successful stage run.
    */
  def buildOutputMap(inputs: Seq[Input], opts: Opts): Map[String, Set[Input]] = {
    val inputToOutputs = inputs.map { input =>
      input -> getOutputs(input)
    }

    // get the list of output -> input
    val outputs = inputToOutputs.flatMap {
      case (input, Outputs.Named(out @ _*)) => out.toList.map(_ -> input)
      case _                                => Seq.empty
    }

    // group the inputs together by output name
    val outputMap = outputs.groupBy(_._1).view.mapValues(_.map(_._2))

    // find the unique list of inputs that should be in ALL outputs
    val inputsInAllOutputs = inputToOutputs
      .filter(_._2 == Outputs.All)
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
    * a mapping of output -> Set[Input].
    */
  def getWork(opts: Opts): Map[String, Set[Input]] = {
    val lastOutputs = if (opts.reprocess()) Seq.empty else Runs.resultsOf(this)

    // get all the outputs that have already been processed
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

  /** Complete all work and write to the database what was done. */
  def insertRuns(outputs: Map[String, Set[Input]]): Unit = {
    for ((output, inputs) <- outputs.toList.sortBy(_._1)) {
      logger.info(s"Updating output $output for $getName ($inputs.size inputs)...")
      Runs.insert(this, output, inputs.toList)
    }
  }

  /** Logs the set of outputs this processor will build if run. */
  def showWork(opts: Opts): Unit = {
    logger.info(s"Finding new/updated inputs of stage $getName...")

    // find all the outputs that need built
    val outputMap = getWork(opts)

    // output them to the log
    if (outputMap.isEmpty) {
      logger.info(s"Stage $getName is up to date.")
    } else {
      for ((output, inputs) <- outputMap) {
        logger.info(s"Output $output has ${inputs.size} new/updated inputs")
      }
    }
  }

  /** Run this stage. */
  def run(opts: Opts): Unit = {
    logger.info(s"${if (opts.test()) "Testing" else "Running"} stage $getName...")

    // find all the outputs that need built
    val outputMap = getWork(opts)

    // process them
    if (outputMap.isEmpty) {
      logger.info(s"$getName is up to date.")
    } else {
      if (!opts.insertRuns()) processOutputs(outputMap.keys.toSeq, opts)
      if (!opts.noInsertRuns()) insertRuns(outputMap)
    }
  }
}
