package org.broadinstitute.dig.aggregator.core

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

/** Companion object with Outputs implementations. */
object Outputs {

  /** Special case to identify everything previously output. */
  final object All extends Outputs

  /** Hand-named outputs. */
  final case class Named(seq: String*) extends Outputs

  /** Special case to identify an output that isn't generated. */
  final object Null extends Outputs
}
