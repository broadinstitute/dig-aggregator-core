package org.broadinstitute.dig.aggregator.core

import cats.effect._

import java.net.URI
import java.nio.file.Paths

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.io.Source
import scala.util.Try

import com.amazonaws.services.elasticmapreduce.model.StepState
import com.amazonaws.services.elasticmapreduce.model.StepSummary
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.model.S3Object

object Implicits {

  /** Needed for IO.sleep. */
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  /** Needed for IO.parSequence. */
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
}
