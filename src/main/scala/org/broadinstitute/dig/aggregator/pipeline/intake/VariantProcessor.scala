package org.broadinstitute.dig.aggregator.pipeline.intake

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors._

import org.json4s._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._

/**
 * Handles uploading of variant topic datasets to HDFS.
 */
class VariantProcessor(name: Processor.Name, config: BaseConfig)
    extends UploadProcessor[VariantProcessor.Variant](name, config) {

  /**
   * Topic to consume.
   */
  override val topic: String = "variants"
}

/**
 * Companion object.
 */
object VariantProcessor {

  /**
   * The data that is deserialized from vartiant dataset UPLOAD messages.
   */
  case class Variant(
      chromosome: String,
      position: Int,
      reference: String,
      allele: String,
      phenotype: String,
      ancestry: String,
      pValue: Double,
      beta: Double,
      freq: Double,
  )
}
