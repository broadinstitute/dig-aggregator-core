package org.broadinstitute.dig.aggregator.core

import java.net.URI

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.ClusterDef
import org.broadinstitute.dig.aws.emr.configurations.Spark

/** Each processor has a globally unique name and a run function. */
abstract class Stage(implicit context: Context) extends LazyLogging {

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

  /** The cluster definition to instantiate for this processor. This is a
    * very basic configuration that should work for a good number of jobs.
    */
  def cluster: ClusterDef = ClusterDef(
    name = getName,
    applicationConfigurations = Seq(
      new Spark.Env().usePython3(),
      new Spark.Config().maximizeResourceAllocation(),
    )
  )

  /** All the new/updated sources that will be checked in S3. These will
    * be applied to the rules of this stage to determine the final set
    * of outputs that need to be built.
    */
  val sources: Seq[Input.Source]

  /** Rules define what inputs (dependencies) are used to build various
    * outputs the stage produces. Whenever an input
    */
  val rules: PartialFunction[Input, Outputs]

  /** Given an output, returns a sequence of job steps to be executed on
    * the cluster.
    */
  def make(output: String): Seq[JobStep]

  /** Get a cached resource file URI.
    *
    * If not already cached, this function will UPLOAD the given resource
    * to S3 and cache the URI to it.
    */
  def resourceUri(resource: String): URI = {
    val cachedUri = resourceMap.getOrElse(
      resource, {
        val key = s"resources/${context.method.getName}/$getName/$resource"

        context.s3.putResource(key, resource)
        context.s3.s3UriOf(key)
      }
    )

    // keep the cache up to date
    resourceMap += resource -> cachedUri
    cachedUri
  }

  /** Process a set of run results. */
  def processOutputs(output: Seq[String], opts: Opts): Unit = {
    val jobs = output.map(output => output -> make(output))

    if (jobs.nonEmpty) {
      val prefix = if (opts.test()) "test" else "out"

      /* Create a set of environment variables for all the steps.
       *
       * For PySpark steps, this is done using the yarn-env configuration.
       * Scripts can also access the environment using `yarn` command.
       */
      val env = Map(
        "JOB_BUCKET" -> s"s3://${context.s3.bucket}",
        "JOB_METHOD" -> context.method.getName,
        "JOB_STAGE"  -> getName,
        "JOB_PREFIX" -> s"$prefix/${context.method.getName}/$getName"
      )

      // upload all additional resources before running
      additionalResources.foreach(resourceUri)

      // spins up the cluster(s) and runs all the job steps
      context.emr.runJobs(cluster, env, jobs.map(_._2))
    }
  }

  /** Using all the outputs returned from `getOutputs`, build a map of
    * output -> Set[(key, eTag)], which will be written to the database
    * after a successful stage run.
    */
  def buildOutputMap(inputs: Seq[Input], opts: Opts): Map[String, Set[Input]] = {
    val inputToOutputs = inputs.map { input =>
      input -> rules.apply(input)
    }

    // get the list of output -> input
    val outputs = inputToOutputs.flatMap {
      case (input, Outputs.Named(out @ _*)) => out.toList.map(_ -> input)
      case _                                => Seq.empty
    }

    // group the inputs together by output name
    val outputMap = outputs.groupBy(_._1).view.mapValues(_.map(_._2).toSet)

    // find the unique list of inputs that should be in ALL outputs
    val inputsInAllOutputs = inputToOutputs
      .filter(_._2 == Outputs.All)
      .map(_._1)
      .toSet

    // append any inputs that belong to ALL outputs to each of them
    val finalMap = outputMap.mapValues(_ ++ inputsInAllOutputs)

    // get all inputs represented in all the outputs
    val allOutputInputs = finalMap.values.flatten.toSet

    // validate that ALL inputs are represented in at least one output
    require(inputs.forall(allOutputInputs.contains))

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
    logger.info(s"Finding new/updated inputs for $getName...")

    // load all the outputs previously written by this stage
    val lastOutputs = if (opts.reprocess()) Seq.empty else Runs.of(this)

    // get all the outputs that have already been processed
    val inputs    = sources.flatMap(source => source.inputs())
    val outputMap = buildOutputMap(inputs, opts)

    // optionally show inputs
    inputs match {
      case Nil                    => logger.warn("No new or updated inputs found")
      case _ if opts.showInputs() => inputs.foreach(i => logger.info(s"...found input ${i.key}"))
      case _                      => ()
    }

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
            case Some(result) if result.version == input.version => false
            case _                                               => true
          }
        }
    }

    // remove outputs from the map with no inputs
    updatedOutputMap.filter { case (_, inputs) => inputs.nonEmpty }
  }

  /** Complete all work and write to the database what was done. */
  def insertRuns(outputs: Map[String, Set[Input]]): Unit = {
    for ((output, inputs) <- outputs.toList.sortBy(_._1)) {
      logger.info(s"Updating output $output for $getName (${inputs.size} inputs)...")
      Runs.insert(this, output, inputs.toList)
    }
  }

  /** Logs the set of outputs this processor will build if run. */
  def showWork(opts: Opts): Unit = {
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
