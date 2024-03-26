package org.broadinstitute.dig.aggregator.core

import com.typesafe.scalalogging.LazyLogging

import java.time.{Instant, LocalDateTime, ZoneOffset}
import scala.io.Source

case class RunStatus(
  project: String,
  method: String,
  stage: String,
  output: String,
  started: Option[LocalDateTime],
  ended: Option[LocalDateTime],
  created: LocalDateTime,
)

object RunStatus extends LazyLogging {

  /** Implicit conversion from Instant -> MySQL. */
  /** Create the runs table if it doesn't already exist. */
  def migrate()(implicit context: Context): Unit = {
    import context.db.ctx._

    // load the create table query
    val sql = Source.fromResource("runstatus.sql").mkString

    // create the table if it doesn't exist
    context.db.ctx.run(quote(infix"#$sql".as[Action[Unit]]))
    ()
  }

  /** Delete the runs table if it exists. Used for testing. */
  private[core] def drop()(implicit context: Context): Unit = {
    import context.db.ctx._

    context.db.ctx.run(quote(infix"DROP TABLE IF EXISTS runs_status".as[Action[Unit]]))
    ()
  }

  /** Returns a list of all runs in the database. Used for testing. */
  private[core] def all()(implicit context: Context): List[RunStatus] = {
    import context.db.ctx._

    context.db.ctx.run(quote(query[RunStatus]))
  }

  /** Delete outputs from the database. */
  def delete(stage: Stage, output: String)(implicit context: Context): Long = {
    import context.db.ctx._

    context.db.ctx.run(quote {
      query[RunStatus].filter { r =>
        r.project == lift(context.project) &&
          r.method == lift(context.method.getName) &&
          r.stage == lift(stage.getName) &&
          r.output == lift(output)
      }.delete
    })
  }

  /** Add outputs to the database with all input versions and provenance data. */
  def insert(stage: Stage, output: String)(implicit context: Context): Long = {
    import context.db.ctx._

    // generate runs to insert
    val runsStatus = RunStatus(
      context.project,
      context.method.getName,
      stage.getName,
      output,
      None,
      None,
      LocalDateTime.ofInstant(Instant.now, ZoneOffset.UTC)
    )

    context.db.ctx.run(quote {
      query[RunStatus].insert(lift(runsStatus))
        .onConflictUpdate(
          (tbl, _) => tbl.started -> None,
          (tbl, _) => tbl.ended -> None,
          (tbl, values) => tbl.created -> values.created,
        )
    })
  }

  def start(stage: Stage, output: String)(implicit context: Context): Unit = {
    import context.db.ctx._

    context.db.ctx.run(quote {
      query[RunStatus].filter { r =>
        r.project == lift(context.project) &&
          r.method == lift(context.method.getName) &&
          r.stage == lift(stage.getName) &&
          r.output == lift(output)
      }.update {
        value => value.started -> lift(Option(LocalDateTime.ofInstant(Instant.now, ZoneOffset.UTC)))
      }
    })
  }

  def end(stage: Stage, output: String)(implicit context: Context): Unit = {
    import context.db.ctx._

    context.db.ctx.run(quote {
      query[RunStatus].filter { r =>
        r.project == lift(context.project) &&
          r.method == lift(context.method.getName) &&
          r.stage == lift(stage.getName) &&
          r.output == lift(output)
      }.update {
        value => value.ended -> lift(Option(LocalDateTime.ofInstant(Instant.now, ZoneOffset.UTC)))
      }
    })
  }

  /** Lookup all the results of the current method's stage. */
  def of(stage: Stage)(implicit context: Context): Seq[RunStatus] = {
    import context.db.ctx._

    context.db.ctx.run(quote {
      query[RunStatus].filter { r =>
        r.project == lift(context.project) &&
          r.method == lift(context.method.getName) &&
          r.stage == lift(stage.getName)
      }
    })
  }
}
