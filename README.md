# DIG Aggregator Core Library

This is a library that all DIG Java/Scala applications that talk to Kafka or AWS can use to have a working baseline. 

The foundational code has the following features:

* Command line parsing;
* Configuration file loading;
* Email notifications;
* [Kafka][kafka] clients: consumers and producers;
* [AWS][aws] clients: [S3][s3] and [EMR][emr];
* [MySQL][mysql] client;
* [Neo4j][neo4j] driver;

It guarantees safety by running all actions through an [IO monad][io]. This ensures that the code - where applicable - can be run concurrently with other code and failures are handled gracefully.

## Usage

### Running tests.  

To run the unit tests, run `sbt test`; to run the integration tests, run `sbt it:test`.

### Building a jar

To build a jar, run `sbt publishLocal`.

Once the JAR is part of your program you can import it like so:

```scala
import org.broadinstitute.dig.aggregator.core._
```

## Command line argument parsing

The `Opts` class can be derived from to include custom command line options. But - out of the box - the following command line options exist:

* `--config [file]` - loads a private, JSON, configuration file (default=`config.json`).
* `--reset` - force a reset of the topic consumer offsets.

_The `Opts` class is used by the other classes to initialize with private data settings that should **not** be committed to the repository: keys, passwords, IP addresses, etc._ 

## Configuration file (JSON) loading

The configuration class _must_ derive from the trait `BaseConfig` as this ensures that a basic JSON structure exists for the rest of the core classes can extract initialization parameters from. There is a default implementation of `BaseConfig` (aptly named `Config`). This is the core template that it loads and expects to exist:

```json
{
    "app": "Unique App Name",
    "kafka": {
        "brokers": [
            "ec2-xx-xx-xx-xx.compute-1.amazonaws.com:9092"
        ]
    },
    "aws": {
        "key": "key",
        "secret": "secret",
        "region": "US_EAST_1",
        "emr": {
            "cluster": "j-cluster-id"
        },
        "s3": {
            "bucket": "s3-bucket-name"
        }
    },
    "mysql": {
        "driver": "com.mysql.cj.jdbc.Driver",
        "url": "xx.xx.rds.amazonaws.com:3306",
        "schema": "db",
        "user": "username",
        "password": "password"
    },
    "neo4j": {
        "url": "xx.xx.rds.amazonaws.com:7687",
        "user": "neo4j",
        "password": "neo4j"
    },
    "sendgrid": {
      "key": "SG.g2l1uOvZQFakEqx4Dcvapw.i7q9pcY5i0eRoFyVmXr59utdVh78h36ob-bcN_9CqLU",
      "from": "do-not-reply@broadinstitute.org",
      "emails": [
        "me@broadinstitute.org",
        "someone@broadinstitute.org"
      ]
    }
}
```

This is where all "private" configuration settings should be kept, and **not** committed to the repository.

## Pure DigApp

The entire application is always run within a pure IO context. This context is handled for you via the `DigApp` abstract class that is intended to have a `run` method implemented in:

```scala
object Main extends DigApp[Config] {

  /**
   * Entry point is automatically called by DigApp.
   */
  override def run(opts: Opts[Config]): IO[ExitCode] = {
    IO.pure(ExitCode.Success)
  }
}
```

The `DigApp` class automatically handles failures and emailing application crashes to the appropriate individuals.

## Options + Configuration File

The command line options are automatically parsed by `DigApp`, but the configuration class used to run the program is passed in and parsed by `Opts` and sent to the `run` method.

If you want to subclass `BaseConfig` to extend the JSON properties your application uses you can (hence the template parameter).

Now you can use your custom options and configuration.

## Kafka Consumer

Use the `Opts` to create a `Consumer`, which can be used to consume all records from a given topic. Example Scala code:

```scala
override def run(opts: Opts[Config]): IO[ExitCode] = {
  
  /* Create a kafka consumer.
   *
   * As the records are consumed and the consumer's state is
   * updated, and all the partition offsets are written to the
   * `partitions` table in the database. Without the --reset flag,
   * the consumer will pick up from where it left off.
   *
   * All partition offsets in the database are keyed by the `app`
   * name in the configuration file and the `topic` name used when
   * the consumer was created.
   */
  val consumer = new Consumer(opts.config, "topic")

  /* Load the current state from the database (or reset to a known
   * good state) and begin consuming all records in the topic.
   * 
   * Technically this runs forever, but we return success.
   */
  consumer.consume(process(consumer)).as(ExitCode.Success)
}

/* This is where the code should do something with the records 
 * received from Kafka. All processing must be done from within
 * the IO monad.
 */
def process(consumer: Consumer)(recs: Consumer#Records): IO[Unit] = {
  val ios = for (rec <- recs.iterator.asScala) yield IO {
    println(s"Processed partition ${rec.partition} offset ${rec.offset}")
  }
  
  /* It's critical that the records for each partition be processed 
   * IN ORDER! However, with a little extra work, records from 
   * different partitions can be run in parallel.
   */
  ios.toList.sequence >> IO.unit
}
```

## Kafka Producer

In addition to a `Consumer`, there is a `Producer` class that can be used to send messages to a topic. Example Scala code:

```scala
override def run(opts: Opts[Config]): IO[ExitCode] = {

  /* Create a kafka producer. It is always assumed that the key and
   * value are both Strings.
   */
  val producer = new Producer(opts.config, "topic")

  /* Send a message to the topic. Again, sending a message runs in the
   * IO monad so that it can be combined safely with consuming and other
   * operations (e.g. writing to a database, S3, ...).
   */
  producer.send("key", """{"key": "value"}""").as(ExitCode.Success)
}
```

## AWS Client (S3 + EMR)

An Amazon Web Services ([AWS][aws]) object can be created, which will initialize both [S3][s3] and [EMR][emr] clients. The [S3][s3] client can be used to perform [CRUD][crud] actions on files stored in the Amazon cloud. And the [EMR][emr] client can be used to execute jobs on the [Hadoop][hadoop] cluster.

### Using S3

There are some simple S3 file system functions available in the `AWS` object:

* `exists(key)` - true if the key exists
* `put(key, contents)` - writes a new key to the bucket 
* `put(key, stream)` - uploads a new key to the bucket
* `get(key)` - returns an `S3Object` for the given key (use `.read` to download)
* `rm(key)` - deletes the key (if present)
* `ls(key)` - recursively lists all keys within a given key
* `rmdir(key)` - recursively delete all keys within a given key
* `mkdir(key, metadata)` - create a new directory key with a `metadata` file in it

_The implicit `bucket` parameter that is passed to all [S3][s3] and [EMR][emr] methods is the one found in the configuration file._

### Spawning Jobs

Jobs on the cluster consist of 1 or more "steps". The following `JobStep` instances exist:

* `MapReduce` - a Hadoop [mapreduce][mr] JAR
* `PySpark` - runs a Python3 script as a [Spark][spark] application
* `Pig` - runs a [Pig][pig] script
* `Script` - runs any shell script (e.g. Perl)

Simply create a list of steps and call `runJob`:

```scala
def doMultiStepJob(aws: AWS): IO[Unit] = {
  val steps = List(
    JobStep.PySpark(new URI("s3://bucket/path/to/script.py"), "arg", "arg"),
    JobStep.Script(new URI("s3://bucket/path/to/script.pl"), "arg", "arg"),
    JobStep.Pig(new URI("s3://bucket/path/to/script.pig", "key" -> "value")),
  )

  // this starts the job and waits for it to complete (or fail)
  val job = aws.runJob(steps) >>= aws.waitForJob

  // assert that the job completed successfully
  job.map(result => assert(result.isRight))
}
```

Waiting for a job to complete waits until either all the steps have completed or any of of the jobs failed, was cancelled, or otherwise interrupted.

## MySQL

TODO: Talk about [doobie][doobie], `config.mysql.newTransactor()` and running queries.

## Neo4j Driver

TODO: Talk about [Neo4j][neo4j], `config.neo4j.newDriver()` and running queries.

# fin.

[scala]: https://scala-lang.org/
[io]: https://typelevel.org/cats-effect/datatypes/io.html
[aws]: https://aws.amazon.com/
[kafka]: https://kafka.apache.org/
[hadoop]: https://hadoop.apache.org/
[crud]: https://en.wikipedia.org/wiki/Create,_read,_update_and_delete
[mr]: https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html
[s3]: https://aws.amazon.com/s3/
[emr]: https://aws.amazon.com/emr/
[mysql]: https://www.mysql.com/
[doobie]: https://tpolecat.github.io/doobie/
[spark]: http://spark.apache.org/
[pig]: http://pig.apache.org/
[mr]: https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
[neo4j]: https://neo4j.com
