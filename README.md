# DIG Aggregator Core Library

This is a core baseline library that all DIG Java/Scala applications can use to have a working baseline with. 

The foundational code has the following features:

* Command line parsing;
* Configuration file loading;
* [Kafka][kafka] clients: consumers and producers;
* [AWS][aws] clients: [S3][s3] and [EMR][emr];
* RDBS clients;

It guarantees safety by running all actions through an [IO monad][io]. This ensures that the code - where applicable - can be run concurrently with other code and failures are handled gracefully.

## Usage

TODO: Describe adding this repository as an SBT library dependency.

Once the JAR is part of your program you can import it like so:

```scala
import org.broadinstitute.dig.aggregator.core._
```

## Command line argument parsing

The `Opts` class can be derived from to include custom command line options. But - out of the box - the following command line options exist:

* `--config [file]` - loads a private, JSON, configuration file (default=`config.json`).
* `--from-beginning` - start topic consumption from the beginning.
* `--continue` - start topic consumption from where it left off (saved in a state file)

_The `Opts` class is used by the other classes to initialize with private data settings that should **not** be committed to the repository: keys, passwords, IP addresses, etc._ 

## Configuration file (JSON) loading

The configuration class _must_ derive from the trait `BaseConfig` as this ensures that a basic JSON structure exists for the rest of the core classes can extract initialization parameters from. There is a default implementation of `BaseConfig` (aptly named `Config`). This is the core template that it loads and expects to exist:

```json
{
    "kafka": {
        "brokers": [
            "ec2-xx-xx-xx-xx.compute-1.amazonaws.com:9092"
        ],
        "consumers": {
            "topic": "state_file.json"
        }
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
    }
}
```

This is where all "private" configuration settings should be kept, and **not** committed to the repository.

## Extending Options + Configuration File

Since the `Opts` object is used by all the other classes to initialize and setup private data, it should be the first thing created:

```scala
def main(args: Array[String]): Unit = {
  val opts = new Opts[Config](args)
  
  // parse the command line arguments
  opts.verify
}
```

If you want to subclass `Config` to extend the JSON properties your application uses you can (hence the template parameter), but you can also extend the `Opt` class to add custom command line options as well.

```scala
case class MyConfig(
  kafka: KafkaConfig,   // required by BaseConfig
  aws: AWSConfig,       // required by BaseConfig
  
  password: String,     // extended parameter
) extends BaseConfig

class MyOpts() extends Opts[MyConfig] {
  val myArg = opt[String]("my-arg")
}
```

Now you can use your custom options and configuration.

## Kafka Consumer

Use the `Opts` class to create a `Consumer`, which can be used to consume all records from a given topic. Example Scala code:

```scala
def main(args: Array[String]): Unit = {
  val opts = new Opts[Config](args)
  
  // parse the command line arguments
  opts.verify

  /* Create a kafka consumer.
   *
   * If --continue was specified, then the offsets in the 
   * configuration topic's state file will be used.
   * 
   * Otherwise the state file will be ignored and either it 
   * will start over at the beginning if --from-beginning 
   * was specified or from the end (the default).
   * 
   * As the records are consumed and the consumer's state is
   * updated, the state file for the topic will be written to
   * disk for later use with --continue.
   */
  val consumer = new Consumer[Config](opts, "topic")

  /* Create a stream that reads all the records from the Kafka 
   * topic and sends them to a function for processing.
   */
  val io = consumer.consume(process)

  // run the program
  io.unsafeRunSync
}

/* This is where the code should do something with the records 
 * received from Kafka. All processing must be done from within
 * the IO monad.
 */
def process(consumer: Consumer[Config], recs: ConsumerRecords[String, String]): IO[Unit] = {
  val ios = for (rec <- recs.iterator.asScala) {
    val io = IO {
      println(s"Processed partition ${rec.partition} offset ${rec.offset}")
    }

    /* Since this record has been successfully processed, the
     * consumer's state should be updated to reflect that so
     * if the program is restarted (with --continue) the same
     * records won't be processed again.
     */
    io >> consumer.updateState(rec)
  }
  
  /* It's critical that the records for each partition be processed 
   * IN ORDER! However, with a little extra work, records from 
   * different partitions can be run in parallel.
   */
  ios.toList.sequence
}
```

## Kafka Producer

In addition to a `Consumer`, there is a `Producer` class that can be used to send messages to a topic. Example Scala code:

```scala
def main(args: Array[String]): Unit = {
  val opts = new Opts[Config](args)
  
  // parse the command line arguments
  opts.verify

  /* Create a kafka producer. It is always assumed that the key and
   * value are both Strings.
   */
  val producer = new Producer[Config](opts, "topic")

  /* Send a message to the topic. Again, sending a message runs in the
   * IO monad so that it can be combined safely with consuming and other
   * operations (e.g. writing to a database, S3, ...).
   */
  val io = producer.send("key", """{"key": "value"}""")

  // run the program
  io.unsafeRunSync
}
```

## AWS Client (S3 + EMR)

An Amazon Web Services ([AWS][aws]) object can be created, which will initialize both [S3][s3] and [EMR][emr] clients. The [S3][s3] client can be used to perform [CRUD][crud] actions on files stored in the Amazon cloud. And the [EMR][emr] client can be used to execute [mapreduce][mr] queries with [Hadoop][hadoop] on those files.

```scala
def main(args: Array[String]): Unit = {
  val opts = new Opts[Config](args)
  
  // parse the command line arguments
  opts.verify

  /* Create an AWS object with both S3 and EMR clients.
   */
  val aws = new AWS[Config](opts)

  /* Add a file (key) to S3.
   */
  val ioPut = aws.put("key", "value")

  /* Perform a mapreduce. This requires that the Java program being run
   * is a JAR assembly stored on S3.
   */
  val ioMR = aws.runMR(
    "s3://bucket/app.jar",      // location of mapreduce program
    "main.Class",               // main class to execute
    args = List(
      "s3://bucket/path/",      // input path
      "s3://bucket/out/path/",  // output path
    )
  )

  // run the program
  (ioPut >> ioMR).unsafeRunSync
}
```

The following [S3][s3] "file system" methods are currently supported:

* `put(key: String, value: String)`
* `get(key: String)`
* `ls(key: String, recursive: Boolean=true, pathSep: Char='/')`
* `rm(keys: Seq[String])`

The only [EMR][emr] method currently available is:

* `runMR(jar: String, mainClass: String, args: List[String])`

_The implicit `bucket` parameter that is passed to all [S3][s3] and [EMR][emr] methods is the one found in the configuration file._

## RDBS Access

TODO: Add support for Doobie and MySQL.

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
