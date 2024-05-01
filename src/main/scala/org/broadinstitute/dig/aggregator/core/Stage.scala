package org.broadinstitute.dig.aggregator.core

import com.typesafe.scalalogging.LazyLogging
import java.net.URI

import org.broadinstitute.dig.aws.emr._
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
  def cluster: ClusterDef = {
    val applicationConfigurations = Seq(
      new Spark.Env().usePython3(),
      new Spark.Config().maximizeResourceAllocation()
    )

    context.config match {
      case Some(config) => {
        import config.aws.emr

        ClusterDef(
          name = getName,
          applicationConfigurations = applicationConfigurations,
          masterEbsVolumeType = emr.masterEbsVolumeType,
          slaveEbsVolumeType = emr.slaveEbsVolumeType
        )
      }
      case None => {
        logger.warn("Config instance missing; assuming default values for needed fields")

        ClusterDef(
          name = getName,
          applicationConfigurations = applicationConfigurations,
        )
      }
    }
  }

    

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
  def make(output: String): Job

  /** Called before actually spinning up clusters and running jobs. If there
    * is any special work that needs to be done before building a job output,
    * do it here.
    */
  def prepareJob(output: String): Unit = ()

  /** For every job that successfully ran, call this function with the
    * output produced.
    */
  def success(output: String): Unit = ()

  /** Get a cached resource file URI.
    *
    * If not already cached, this function will UPLOAD the given resource
    * to S3 and cache the URI to it.
    */
  def resourceUri(resource: String): URI = {
    val cachedUri = resourceMap.getOrElse(resource, {
      val key = s"resources/${context.method.getName}/$resource"

      context.s3.putResource(key, resource)
      context.s3.s3UriOf(key)
    })

    // keep the cache up to date
    resourceMap += resource -> cachedUri
    cachedUri
  }

  /** Build the jobs to produce the output map and run them. */
  def processOutputs(output: Map[String, Set[Input]], opts: Opts): Unit = {
    val jobs = output.keys.map(make)

    // special case when there's nothing to do
    if (jobs.nonEmpty) {
      val prefix = if (opts.test()) "test" else "out"

      /* Create a set of environment variables for all the steps.
       *
       * For PySpark steps, this is done using the yarn-env configuration.
       * Scripts can also access the environment using `yarn` command.
       */
      var env: Map[String, String] = Map(
        "JOB_BUCKET" -> s"s3://${context.s3.bucket}",
        "INPUT_PATH" -> s"s3://${context.s3.path}",
        "OUTPUT_PATH" -> s"s3://${context.s3Output.path}",
        "BIOINDEX_PATH" -> s"s3://${context.s3Bioindex.path}",
        "PORTAL_SECRET" -> context.config.get.aws.portal.instance,
        "PORTAL_DB" -> context.portal.secret.get.dbname,
        "JOB_METHOD" -> context.method.getName,
        "JOB_STAGE"  -> getName,
        "JOB_PREFIX" -> s"$prefix/${context.method.getName}/$getName",
        "PROJECT" -> context.project
      )

      // if --test, set the dry run environment variable
      if (opts.test()) {
        env += "JOB_DRYRUN" -> "true"
      }

      // upload all additional resources before running
      additionalResources.foreach(resourceUri)

      // add a bootstrap step to the cluster for installing things needed by the aggregator
      val clusterCommon = {
        val common  = new BootstrapScript(resourceUri("common-bootstrap.sh"))
        val scripts = cluster.bootstrapScripts :+ common

        cluster.copy(bootstrapScripts = scripts)
      }

      // prepare to run all the jobs
      output.keys.foreach(prepareJob)
      output.keys.foreach(o => RunStatus.insert(this, o))
      output.keys.foreach(o => RunStatus.start(this, o))

      // spins up the cluster(s) and runs all the jobs
      context.emr.runJobs(clusterCommon, env, jobs.toSeq, maxParallel = opts.maxClusters())

      // jobs all completed successfully, perform success function
      output.keys.foreach(success)
    }
  }

  /** Using all the outputs returned from `getOutputs`, build a map of
    * output -> Set[(key, eTag)], which will be written to the database
    * after a successful stage run.
    */
  def buildOutputMap(inputs: Seq[Input], opts: Opts): Map[String, Set[Input]] = {
    val inputToOutputs = inputs.map { input => input -> rules.apply(input) }

    // get the list of output -> input
    val outputs = inputToOutputs.flatMap {
      case (input, Outputs.Named(out @ _*)) => out.toList.map(_ -> input)
      case _                                => Seq.empty
    }

    // get the list of all inputs that mapped to the null output
    val ignoredInputs = inputToOutputs.collect {
      case (input, Outputs.Null) => input
    }

    // group the inputs together by output name
    val outputMap = outputs.groupBy(_._1).view.mapValues(_.map(_._2).toSet)

    // find the unique list of inputs that should be in ALL outputs
    val inputsInAllOutputs = inputToOutputs
      .filter(_._2 == Outputs.All)
      .map(_._1)
      .toSet

    // append any inputs that belong to ALL outputs to each of them
    val finalMap = outputMap.mapValues(_ ++ inputsInAllOutputs).toMap

    // get all inputs represented in all the outputs
    val allOutputInputs = finalMap.values.flatten.toSet

    // get all the inputs that are NOT in at least one output
    val missedInputs = inputs.filterNot(allOutputInputs.contains)

    // validate that ALL missed inputs are intentionally ignored
    if (!missedInputs.forall(ignoredInputs.contains)) {
      logger.error("Some inputs are NOT represented in the final outputs:")

      // dump the list of inputs not represented
      for (input <- missedInputs.filterNot(ignoredInputs.contains)) {
        logger.error(input.key)
      }

      // no work will be done!
      Map.empty
    } else {
      finalMap
        .filter { case (output, _) => opts.onlyGlobs.forall(_.exists(_.matches(output))) }
        .filterNot { case (output, _) => opts.excludeGlobs.exists(_.exists(_.matches(output))) }
    }
  }

  /** Determines the set of things that need to be processed. This is
    * a mapping of output -> Set[Input].
    */
  def getWork(opts: Opts): Map[String, Set[Input]] = {
    if (opts.reprocess()) {
      logger.info(s"Finding all inputs for $getName...")
    } else {
      logger.info(s"Finding new/updated inputs for $getName...")
    }

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

        // remove inputs that have already been processed
        val newInputs = inputs.filter { input =>
          results.find(_.input == input.key) match {
            case Some(result) if result.timestamp.isAfter(input.version) => false
            case _                                                       => true
          }
        }

        // update the map of outputs to new and updated inputs
        output -> newInputs
    }

    // remove outputs from the map with no inputs
    updatedOutputMap.filter { case (_, inputs) => inputs.nonEmpty }
  }

  /** Complete all work and write to the database what was done. */
  def insertRuns(outputs: Map[String, Set[Input]]): Unit = {
    for ((output, inputs) <- outputs.toList.sortBy(_._1)) {
      logger.info(s"Updating output $output for $getName (${inputs.size} inputs)...")
      Runs.insert(this, output, inputs.toList)
      RunStatus.end(this, output)
    }
  }

  /** Logs the set of outputs this processor will build if run. Returns true
    * if there are new/update inputs that need to be processed.
    */
  def showWork(opts: Opts): Boolean = {
    val outputMap = getWork(opts)

    // output them to the log
    if (outputMap.isEmpty) {
      logger.info(s"Stage $getName is up to date.")
    } else {
      for ((output, inputs) <- outputMap) {
        logger.info(s"Output $output has ${inputs.size} new/updated inputs")
      }
    }

    // true if there is work to do
    outputMap.nonEmpty
  }

  /** Run this stage. */
  def run(opts: Opts): Unit = {
    logger.info(s"${if (opts.test()) "Testing" else "Running"} stage $getName...")

    // find all the outputs that need built
    getWork(opts) match {
      case outputMap if outputMap.isEmpty => ()
      case outputMap if opts.insertRuns() =>
        outputMap.keys.foreach(o => RunStatus.insert(this, o))
        outputMap.keys.foreach(o => RunStatus.start(this, o))
        insertRuns(outputMap)
        outputMap.keys.foreach(success)

      case outputMap =>
        processOutputs(outputMap, opts)

        // finally, write the runs to the database
        if (!opts.noInsertRuns()) {
          insertRuns(outputMap)
        }
    }
  }
}
