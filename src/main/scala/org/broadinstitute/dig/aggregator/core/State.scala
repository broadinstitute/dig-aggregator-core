package org.broadinstitute.dig.aggregator.core

import cats._
import cats.effect._

import fs2._

import java.io.File
import java.io.PrintWriter
import java.util.Properties

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common._

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, writePretty}

import scala.collection.JavaConverters._
import scala.io.Source

/**
 * The last offset processed per partition.
 */
case class PartitionState(partition: TopicPartition, offset: Long) {

  /**
   * Helper function for matching a TopicPartition.
   */
  def matches(topic: String, partition: Int) = {
    this.partition.topic.equals(topic) && this.partition.partition == partition
  }
}

/**
 * A list of topic partition offsets.
 */
case class ConsumerState(partitions: List[PartitionState]) {

  /**
   * Returns a new ConsumerState with an updated offset for a topic partition.
   */
  def withOffset(topic: String, partition: Int, offset: Long): ConsumerState = {
    val updatedPartitions = partitions.map {
      case state if state.matches(topic, partition) => state.copy(offset=offset)
      case state                                    => state
    }

    // return a new consumer state
    this.copy(partitions = updatedPartitions)
  }
}

/**
 * Companion object for creating, loading, and saving ConsumerState instances.
 */
object State {
  implicit val formats = DefaultFormats

  /**
   * The partition offsets to start consuming from.
   */
  sealed trait Position

  /** Start from the beginning, end, or continue from known offsets. */
  final case object Beginning extends Position
  final case object End extends Position
  final case object Continue extends Position

  /**
   * Create a new ConsumerState that starts from the beginning offset.
   */
  def fromBeginning(client: KafkaConsumer[String, String], partitions: Seq[TopicPartition]) = {
    val offsets = client.beginningOffsets(partitions.asJava).asScala map {
      case (topicPartition, offset) => PartitionState(topicPartition, offset)
    }

    // create the new state
    ConsumerState(offsets.toList)
  }

  /**
   * Create a new ConsumerState that starts from the last offset.
   */
  def fromEnd(client: KafkaConsumer[String, String], partitions: Seq[TopicPartition]) = {
    val offsets = client.endOffsets(partitions.asJava).asScala map {
      case (topicPartition, offset) => PartitionState(topicPartition, offset)
    }

    // create the new state
    ConsumerState(offsets.toList)
  }

  /**
   * Load a ConsumerState from a JSON file.
   */
  def load(file: File) = {
    read[ConsumerState](Source.fromFile(file).mkString)
  }

  /**
   * Save a ConsumerState to a JSON file.
   */
  def save(state: ConsumerState, file: File) = {
    val json = writePretty(state)
    val writer = new PrintWriter(file)

    writer.write(json)
    writer.close
  }
}
