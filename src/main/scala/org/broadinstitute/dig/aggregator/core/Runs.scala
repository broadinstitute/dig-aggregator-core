package org.broadinstitute.dig.aggregator.core

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
  *
  * This class is only used by quill to insert rows and is never used
  * outside of insertion. Use `Runs.Result` for getting data out of the
  * `Runs` table.
  */
case class Runs(
    method: String,
    stage: String,
    input: String,
    version: String,
    output: String,
    source: Option[String],
    branch: Option[String],
    commit: Option[String],
)

/** Companion object for determining what inputs have been processed and
  * have yet to be processed.
  */
object Runs extends LazyLogging {

  /** Create the runs table if it doesn't already exist. */
  def migrate(): Unit = {
    val db  = Context.current.db
    val sql = Source.fromResource("runs.sql").mkString

    import db.ctx._

    db.ctx.run(quote(infix"#$sql".as[Action[Unit]]))
    ()
  }

  /** Delete outputs from the database. */
  def delete(stage: Stage, output: String): Long = {
    val db     = Context.current.db
    val method = Context.current.method

    import db.ctx._

    db.ctx.run(quote {
      query[Runs].filter { r =>
        r.method == lift(method.getName) &&
        r.stage == lift(stage.getName) &&
        r.output == lift(output)
      }.delete
    })
  }

  /** Add outputs to the database with all input versions and provenance data. */
  def insert(stage: Stage, output: String, inputs: Seq[Input]): List[Long] = {
    val db     = Context.current.db
    val method = Context.current.method

    import db.ctx._

    // provenance data for method
    val source = method.provenance.flatMap(_.source)
    val branch = method.provenance.flatMap(_.branch)
    val commit = method.provenance.flatMap(_.commit)

    // generate runs to insert
    val runs = inputs.map { input =>
      Runs(
        method.getName,
        stage.getName,
        input.key,
        input.eTag,
        output,
        source,
        branch,
        commit
      )
    }

    db.ctx.run(quote {
      liftQuery(runs).foreach { r =>
        query[Runs]
          .insert(r)
          .onConflictUpdate(
            (tbl, values) => tbl.version -> values.version,
            (tbl, values) => tbl.source  -> values.source,
            (tbl, values) => tbl.branch  -> values.branch,
            (tbl, values) => tbl.commit  -> values.commit,
          )
      }
    })
  }

  /** Lookup all the results of the current method's stage. */
  def resultsOf(stage: Stage): Seq[Runs] = {
    val db     = Context.current.db
    val method = Context.current.method

    import db.ctx._

    db.ctx.run(quote {
      query[Runs].filter(r => r.method == lift(method.getName) && r.stage == lift(stage.getName))
    })
  }
}
