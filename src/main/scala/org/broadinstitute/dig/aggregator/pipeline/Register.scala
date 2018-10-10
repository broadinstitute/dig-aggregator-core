package org.broadinstitute.dig.aggregator.pipeline

/**
 *
 */
trait ProcessorRegister {

  /**
   * This method does nothing. It's here so that all pipeline registration
   * objects have a guaranteed method that can force the object to load and
   * create all its processor names.
   */
  def register(): Unit = ()
}

/**
 * Imports all the processors from each pipeline to ensure that the names are
 * registered and that there are no conflicts!
 */
object Register {
  def processors(): Unit = {
    intake.Processors.register()
    metaanalysis.Processors.register()
  }
}
