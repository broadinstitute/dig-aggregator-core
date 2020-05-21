package org.broadinstitute.dig.aggregator.core

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import com.typesafe.scalalogging.LazyLogging

import doobie._
import doobie.implicits._

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
  * This class is only used by doobie to insert rows and is never used
  * outside of insertion. Use `Run.Result` for getting data out of the
  * `runs` table.
  */
case class Run(
    method: Method,
    stage: Stage,
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
object Run extends LazyLogging {

  /** A run Input is a S3 key and eTag (checksum) pair. */
  final case class Input(key: String, eTag: String) {

    /** It's common to split an S3 key by path separator. */
    lazy val splitKeys: Array[String] = key.split('/')

    /** Basename is the actual filename of the key. */
    lazy val basename: String = splitKeys.last

    /** Prefix is the directory name of the key. */
    lazy val prefix: String = splitKeys.dropRight(1).mkString("/")
  }

  /** Companion Input object for sources. */
  object Input {
    import org.broadinstitute.dig.aws.Implicits._

    /* An input source an S3 object used to identify all sibling and
     * child objects that can be processed. Typically the key is either
     * "metadata" or "_SUCCESS", but can be a glob.
     *
     * When determining what data needs to be processed, the prefix
     * is used to recursively list all objects in S3 matching key.
     * The ETag of those objects is compared against the ETag of the
     * last time it was processed by the stage.
     */
    sealed trait Source {
      val prefix: String
      val key: Glob

      /** Fetch all the keys matching the input and their e-tags. */
      def objects: Seq[Run.Input] = {
        Method.aws.value
          .ls(prefix)
          .filter(obj => key.matches(obj.key))
          .map(obj => Input(obj.key, obj.eTagStripped))
      }
    }

    /** Companion source object for types of input sources. */
    object Source {

      /** Dataset inputs. */
      final case class Dataset(prefix: String) extends Source {
        override val key: Glob = Glob("metadata")
      }

      /** SUCCESS spark job results. */
      final case class Success(prefix: String) extends Source {
        override val key: Glob = Glob("_SUCCESS")
      }

      /** Binary inputs are programs and raw data files. */
      final case class Binary(prefix: String) extends Source {
        override val key: Glob = Glob.True
      }
    }
  }

  /** Implicit conversion to DB string from Method and Stage for doobie. */
  implicit val MethodPut: Put[Method] = Put[String].tcontramap(_.getName)
  implicit val StagePut: Put[Stage]   = Put[String].tcontramap(_.getName)

  /** Create the runs table if it doesn't already exist. */
  def migrate(pool: DbPool): IO[Unit] = {
    val q = sql"""|CREATE TABLE IF NOT EXISTS `runs`
                  |  ( `id` INT NOT NULL AUTO_INCREMENT
                  |  , `method` VARCHAR(200) NOT NULL
                  |  , `stage` VARCHAR(200) NOT NULL
                  |  , `input` VARCHAR(1024) NOT NULL
                  |  , `version` VARCHAR(80) NOT NULL
                  |  , `output` VARCHAR(200) NOT NULL
                  |  , `timestamp` DATETIME NOT NULL DEFAULT NOW()
                  |  , `source` VARCHAR(1024) NULL
                  |  , `branch` VARCHAR(200) NULL
                  |  , `commit` VARCHAR(40) NULL
                  |  , PRIMARY KEY (`id`)
                  |  , UNIQUE INDEX `stage_IDX` (`method` ASC, `stage` ASC, `input` ASC, `output` ASC)
                  |)
                  |""".stripMargin

    // execute the query
    for {
      _ <- IO(logger.debug(s"Migrating runs table..."))
      _ <- pool.exec(q.update.run).as(())
    } yield ()
  }

  /** Run entries are created and inserted atomically for a single output. */
  def insert(stage: Stage, output: String, inputs: NonEmptyList[Input]): IO[Unit] = {
    val q = s"""|INSERT INTO `runs`
                |  ( `method`
                |  , `stage`
                |  , `input`
                |  , `version`
                |  , `output`
                |  , `source`
                |  , `branch`
                |  , `commit`
                |  )
                |
                |VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                |
                |ON DUPLICATE KEY UPDATE
                |  `version` = VALUES(`version`),
                |  `source` = VALUES(`source`),
                |  `branch` = VALUES(`branch`),
                |  `commit` = VALUES(`commit`),
                |  `timestamp` = NOW()
                |""".stripMargin

    // get the provenance information from the method for this run
    val provenance = Method.current.value.provenance

    // create an entry per input
    val entries = inputs.map { input =>
      Run(
        Method.current.value,
        stage,
        input.key,
        input.eTag,
        output,
        provenance.flatMap(_.source),
        provenance.flatMap(_.branch),
        provenance.flatMap(_.commit)
      )
    }

    // batch insert
    val insert = Update[Run](q).updateMany(entries.toList)

    for {
      _ <- IO(logger.debug(s"Inserting run output '$output' with ${inputs.size} inputs..."))
      _ <- DbPool.current.value.exec(insert)
    } yield ()
  }

  /** When querying the `runs` table to determine what has already been
    * processed by a stage, this is what is returned.
    *
    * The input is the S3 key, which typically maps to an entire directory.
    * For example, "_SUCCESS" for a directory containing Spark output. The
    * version is the S3 ETag (MD5 checksum) of the input key. The output
    * is the unique name provided by the stage when it was processed.
    */
  final case class Result(output: String, input: String, version: String) {
    override def toString: String = output
  }

  /** Lookup all the results of the current method's stage. */
  def resultsOf(stage: Stage): IO[Seq[Result]] = {
    val q = sql"""|SELECT `output`, `input`, `version`
                  |FROM   `runs`
                  |WHERE  `method`=${Method.current.value}
                  |AND    `stage`=$stage
                  |""".stripMargin.query[Result].to[Seq]

    DbPool.current.value.exec(q)
  }
}
