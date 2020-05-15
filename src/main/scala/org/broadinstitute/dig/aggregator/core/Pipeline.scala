package org.broadinstitute.dig.aggregator.core

import java.io.File
import java.net.URLClassLoader

import cats.effect._
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.json4s.DefaultFormats
import org.json4s.Formats
import org.json4s.jackson.Serialization.read

import scala.io.Source

case class ClusterDef(
    masterInstanceType: String,
    slaveInstanceType: Option[String],
    instances: Option[Int],
    masterVolumeSizeInGB: Option[Int],
    bootstrapScripts: Option[Seq[String]],
    bootstrapSteps: Option[Seq[String]],
)

case class ProcessorDef(
    name: String,
    resources: Seq[String],
    dependencies: Seq[String],
    cluster: ClusterDef,
)

/** A pipeline is an object that keeps runtime knowledge of all its processors.
  */
case class Pipeline(jar: String, namespace: String, name: String, processors: Seq[ProcessorDef]) extends LazyLogging {

  /** Load the JAR into the classpath now! */
  val classLoader = new URLClassLoader(Array(new File(jar).toURI.toURL))

  /** Register each of the processor names. This guarantees uniqueness across pipelines. */
  val processorNames: Set[Processor.Name] = {
    processors.map { p =>
      val cls  = classLoader.loadClass(s"$namespace.${p.name}")
      val ctor = cls.getDeclaredConstructor(Class[Processor.Name], Class[BaseConfig], Class[DbPool])

      // instantiate the processor from the loaded class
      def newInstance(name: Processor.Name, config: BaseConfig, pool: DbPool): Processor = {
        ctor.newInstance(name, config, pool).asInstanceOf[Processor]
      }

      // ensure the processor name is unique across all others
      Processor.register(p.name, newInstance)
    }.toSet
  }

  /** Output all the work that each processor in the pipeline has to do. This
    * will only show a single level of work, and not later work that may need
    * to happen downstream.
    */
  def showWork(config: BaseConfig, pool: DbPool, opts: Processor.Opts): IO[Unit] = {
    val allProcessors = processorNames.map(Processor(_)(config, pool).get)

    // get the set of processors that have known work
    val knownWork = Pipeline.getKnownWork(allProcessors, opts)

    // from that, determine the processors that can actually run
    val showWork = knownWork.flatMap { toRun =>
      val processorsToRun = Pipeline.getShouldRun(allProcessors, toRun)

      // output the work for those processors
      processorsToRun.map(_.showWork(opts)).toList.sequence
    }

    showWork >> IO.unit
  }

  /** Run the entire pipeline.
    *
    * This function loops over all the processors looking for any that have
    * work to do and should run, then runs them (in parallel). Once they
    * complete, this is done again. This continues until all processors are
    * done doing their work and have nothing left to do.
    */
  def run(config: BaseConfig, pool: DbPool, opts: Processor.Opts): IO[Unit] = {
    val allProcessors = processorNames.map(Processor(_)(config, pool).get)

    // recursive helper function
    def runProcessors(opts: Processor.Opts): IO[Unit] = {
      import Implicits._

      // get a list of all processors that have known work
      val knownWork: IO[Set[Processor]] = Pipeline.getKnownWork(allProcessors, opts)

      // determine the list of all processors that have work
      knownWork.flatMap { toRun =>
        if (toRun.isEmpty) {
          IO(logger.info("Everything up to date."))
        } else {
          val shouldRun = Pipeline.getShouldRun(allProcessors, toRun)

          // all the processors that can run can do so in parallel
          val io = if (shouldRun.isEmpty) {
            IO.raiseError(new Exception("There's work to do, but nothing ran!"))
          } else {
            shouldRun.map(_.run(opts)).toList.sequence
          }

          // after they finish, recursively try again (don't reprocess!)
          io >> runProcessors(opts.copy(reprocess = false))
        }
      }
    }

    // run until no work is left
    runProcessors(opts)
  }
}

/** Imports all the processors from each pipeline to ensure that the names are
  * registered and that there are no conflicts!
  */
object Pipeline {
  implicit val formats: Formats = DefaultFormats

  /** All loaded pipelines. */
  private var loadedPipelines = Map.empty[String, Pipeline]

  /** Load the pipeline JSON file and parse it. */
  def load(file: File): Unit = {
    val source   = Source.fromFile(file)
    val pipeline = read[Pipeline](source.mkString)

    // ensure a unique name for the pipeline
    require(!loadedPipelines.contains(pipeline.name))

    // add the pipeline to the loaded pipelines
    loadedPipelines = loadedPipelines + (pipeline.name -> pipeline)
    source.close()
  }

  /** Load a JSON array of pipeline files. */
  def loadAll(pipelines: File): Unit = {
    val source = Source.fromFile(pipelines)
    val list   = read[Seq[String]](source.mkString)

    // load them all in order
    list.map(new File(_)).foreach(load)
    source.close()
  }

  /** Lookup a registered pipeline.
    */
  def apply(pipeline: String): Option[Pipeline] = {
    loadedPipelines.get(pipeline)
  }

  /** Given a set of processors, filter those that have work.
    */
  private def getKnownWork(processors: Set[Processor], opts: Processor.Opts): IO[Set[Processor]] = {
    val getWork = processors.toList.map { p =>
      p.hasWork(opts).map(p -> _)
    }

    for (work <- getWork.sequence) yield {
      work
        .filter { case (_, hasWork) => hasWork }
        .map { case (processor, _) => processor }
        .toSet
    }
  }

  /** Find all processors NOT represented in `toRun` that will run due to a
    * a dependency that is in the `toRun` set.
    */
  @scala.annotation.tailrec
  private def getShouldRun(allProcessors: Set[Processor], toRun: Set[Processor]): Set[Processor] = {

    // true if immediate dependency is in the `toRun` set
    def dependencyWillRun(r: Processor) = {
      r.dependencies.exists(dep => toRun.exists(_.name == dep))
    }

    // find all processors not yet set to run that have a dependency that will
    val inferred = (allProcessors -- toRun).filter(dependencyWillRun)

    /* If processors were inferred that will end up having work due to a
     * dependency that will run, add them to the set of processors `toRun` and
     * recurse.
     *
     * Otherwise, return the set of processors in the `toRun` set that either
     * have no dependencies, or dependencies that will not run.
     */

    if (inferred.nonEmpty) {
      getShouldRun(allProcessors, toRun ++ inferred)
    } else {
      toRun.filter(!dependencyWillRun(_))
    }
  }
}
