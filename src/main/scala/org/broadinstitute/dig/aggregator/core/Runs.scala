package org.broadinstitute.dig.aggregator.core

import java.time.LocalDateTime

import com.typesafe.scalalogging.LazyLogging

import scala.io.Source

/** An Run represents a single row in the `runs` table.
  *
  * Each conceptual "run" is a tuple of (method, stage, input) used to
  * generate a single output. The version is paired with the input, and
  * is used to identify whether or not the input has been updated since
  * the last time it was used to generate the output.
  *
  * A stage may take several inputs to produce a single output. In such
  * an instance, multiple records are inserted.
  */
case class Runs(
    method: String,
    stage: String,
    input: String,
    version: String,
    output: String,
    timestamp: LocalDateTime,
)

/** Companion object for determining what inputs have been processed and
  * have yet to be processed.
  */
object Runs extends LazyLogging {

  /** Create the runs table if it doesn't already exist. */
  def migrate()(implicit context: Context): Unit = {
    import context.db.ctx._

    // load the create table query
    val sql = Source.fromResource("runs.sql").mkString

    // create the table if it doesn't exist
    context.db.ctx.run(quote(infix"#$sql".as[Action[Unit]]))
    ()
  }

  /** Delete the runs table if it exists. Used for testing. */
  private[core] def drop()(implicit context: Context): Unit = {
    import context.db.ctx._

    context.db.ctx.run(quote(infix"DROP TABLE IF EXISTS runs".as[Action[Unit]]))
    ()
  }

  /** Returns a list of all runs in the database. Used for testing. */
  private[core] def all()(implicit context: Context): List[Runs] = {
    import context.db.ctx._

    context.db.ctx.run(quote(query[Runs]))
  }

  /** Delete outputs from the database. */
  def delete(stage: Stage, output: String)(implicit context: Context): Long = {
    import context.db.ctx._

    context.db.ctx.run(quote {
      query[Runs].filter { r =>
        r.method == lift(context.method.getName) &&
        r.stage == lift(stage.getName) &&
        r.output == lift(output)
      }.delete
    })
  }

  /** Add outputs to the database with all input versions and provenance data. */
  def insert(stage: Stage, output: String, inputs: Seq[Input])(implicit context: Context): List[Long] = {
    import context.db.ctx._

    // generate runs to insert
    val runs = inputs.map { input =>
      Runs(
        context.method.getName,
        stage.getName,
        input.key,
        input.version,
        output,
        LocalDateTime.now(),
      )
    }

    context.db.ctx.run(quote {
      liftQuery(runs).foreach { r =>
        query[Runs]
          .insert(r)
          .onConflictUpdate(
            (tbl, values) => tbl.version   -> values.version,
            (tbl, values) => tbl.timestamp -> values.timestamp,
          )
      }
    })
  }

  /** Lookup all the results of the current method's stage. */
  def of(stage: Stage)(implicit context: Context): Seq[Runs] = {
    import context.db.ctx._

    context.db.ctx.run(quote {
      query[Runs].filter { r =>
        r.method == lift(context.method.getName) &&
        r.stage == lift(stage.getName)
      }
    })
  }
}
