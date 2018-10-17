package org.broadinstitute.dig.aggregator.pipeline.intake

import cats._
import cats.effect._
import cats.implicits._

import org.broadinstitute.dig.aggregator.core._
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.processors._

class CommitProcessor(name: Processor.Name, config: BaseConfig) extends IntakeProcessor(name, config) {

  /**
   * Topic to consume.
   */
  override val topic: String = "commits"

  /**
   * Commit processors only ever care about the last commit offset and not
   * the offsets of the source topic.
   */
  override def resetState: IO[State] = {
    State.lastCommit(xa, name, None)
  }

  /**
   * Parse each record as a Commit and process the commits.
   */
  override def processRecords(records: Seq[Consumer.Record]): IO[_] = {
    val ios = for (record <- records) yield {
      val commit = Commit.fromRecord(record)

      for {
        _ <- commit.insert(xa)
        _ <- IO(logger.info(s"Committed ${commit.topic} - ${commit.dataset}"))
      } yield ()
    }

    ios.toList.sequence
  }
}
