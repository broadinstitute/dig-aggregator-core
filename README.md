# DIG Aggregator Core Library

This is a core baseline library that all DIG Java/Scala applications can use to have a working baseline with. 

The foundational code has the following features:

* Command line parsing;
* Configuration file loading;
* Kafka clients: consumers and producers;
* AWS clients: S3 and EMR;
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

* `--config [file]` - loads a private, JSON, configuration file.
* `--from-beginning` - start topic consumption from the beginning.
* `--continue` - start topic consumption from where it left off (saved in a state file)

## Configuration file (JSON) loading

The configuration class _must_ derive from `Config` as his will ensure that a basic JSON structure exists that the rest of the core classes can draw from.

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

## Kafka Consumer

Use the `Opts` class to create a `Consumer`, which can be used to consume all records from a given topic. Example Scala code:

```scala
def main(args: Array[String]): Unit = {
  val opts = new Opts[Config](args)
  
  // parse the command line arguments
  opts.verify

  /* Create a kafka consumer. This will use the broker list and 
   * topic specified in the config file loaded by the options.
   *
   * If --continue was specified, then the offsets in the 
   * configuration's stateFile will be used.
   * 
   * Otherwise the stateFile will be ignored and either it will
   * start over at the beginning if --from-beginning was specified
   * or from the end.
   * 
   * Regardless, if stateFile is set within the configuration file,
   * then as the Consumer's state is updated it will be written to 
   * disk for later use with --continue.
   */
  val consumer = new Consumer[Config](opts, "topic")

  /* Create a stream that reads all the records from the Kafka 
   * topic and sends them to a function for processing.
   */
  val stream = consumer.consume(process)

  /* Process the stream forever.
   */
  stream.unsafeRunSync
}

def process(consumer: Consumer[Config], recs: ConsumerRecords[String, String]): IO[Unit] = {

  /* This is where the code should do something with the records 
   * received from Kafka. All processing must be done from within
   * the IO monad.
   */
  val ios = 
    for (rec <- recs.iterator.asScala) {
      val io = IO {
        println(s"Processed partition ${rec.partition} offset ${rec.offset}")
      }

      /* Since these records have been successfully processed, 
       * the consumer's state should be updated to reflect that 
       * so if the program is restarted (with --continue) the 
       * same  records won't be processed again.
       */
      io >> consumer.updateState(rec)
    }
  
  /* It's critical that the records for each partition be processed 
   * IN ORDER! However, with a little extra work, records from different
   * partitions can be run in parallel.
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

  /* Run the program.
   */
  io.unsafeRunSync
}
```

## AWS Client (S3 + EMR)

An Amazon Web Services (AWS) object can be created using the parsed options, which will create both S3 and EMR clients. The S3 client can be used to perform [CRUD][crud] actions on files stored in the Amazon cloud. The EMR client can be used to execute [mapreduce][mr] queries with [Hadoop][hadoop] on those files.

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
  val ioMR = aws.runMR("s3://bucket/app.jar", "main.Class", args = List.empty)

  /* Run the program.
   */
  (ioPut >> ioMR).unsafeRunSync
}
```

The following S3 "file system" methods are currently supported:

* `put(key: String, value: String)`
* `get(key: String)`
* `ls(key: String, recursive: Boolean=true, pathSep: Char='/')`
* `rm(keys: Seq[String])`

The only EMR method currently available is:

* `runMR(jar: String, mainClass: String, args: List[String])`

_The implicit `bucket` parameter that is passed to all S3 and EMR methods is the one found in the configuration file._

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
