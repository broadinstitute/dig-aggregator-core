package org.broadinstitute.dig.aggregator.pipeline.intake

import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors._

import org.json4s._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConverters._
import scala.util._
import com.typesafe.scalalogging.LazyLogging

/**
 * Handles uploading of 1KG topic datasets to HDFS.
 */
class ThousandGenomeProcessor(name: Processor.Name, config: BaseConfig)
    extends UploadProcessor[ThousandGenomeProcessor.Variant](name, config) {

  /**
   * Topic to consume.
   */
  override val topic: String = "1kg"
}

object ThousandGenomeProcessor {

  /** Root information about each variant. */
  case class Variant(
      rsId: String,
      varId: String,
      chromosome: String,
      position: Int,
      reference: String,
      allele: String,
      info: Info,
  )

  /** Optional information fields with each variant. */
  case class Info(
      LDAF: Option[Double],
      AVGPOST: Option[Double],
      RSQ: Option[Double],
      ERATE: Option[Double],
      THETA: Option[Double],
      CIEND: Option[Seq[Int]],
      CIPOS: Option[Seq[Int]],
      END: Option[Int],
      HOMLEN: Option[Int],
      HOMSEQ: Option[String],
      SVLEN: Option[Int],
      SVTYPE: Option[String],
      AC: Option[Int],
      AN: Option[Int],
      AA: Option[String],
      AF: Option[Double],
      AMR_AF: Option[Double],
      ASN_AF: Option[Double],
      AFR_AF: Option[Double],
      EUR_AF: Option[Double],
      VT: Option[String],
      SNPSOURCE: Option[String],
  )
}
