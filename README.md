# DIG Analysis Pipeline

This project contains all the code necessary to run the various intake/data processing pipelines necessary to build the DIG Portal Database. It guarantees safety by running all actions through an [IO monad][io]. This ensures that the code - where applicable - can be run concurrently with other code and failures are handled gracefully.

## Running Tests

To run the unit tests, run `sbt test`; to run the integration tests, run `sbt it:test`.

## Running Processors

To run an individual processor, just run SBT with the processor name.

```bash
sbt "run <processor>"
```

This will show what work (if any) the processor would do. To actually run the processor, you need to provide `--yes` on the command line:

```bash
sbt "run --yes <processor>"
```

Each processor keeps track of what work it has done and what dependencies it has, so at any time it can do only the work that it needs to do. If there is ever a need to force the processor to reprocess work that it has already done, pass `--reprocess` on the command line:

```bash
sbt "run --reprocess <processor>"
```

_Again, note that the above will only show the work that would be done, pass `--yes` along with `--reprocess` to actually do the work._

## Running Pipelines

If `--pipeline` is present on the command line, then instead of a `<processor>` name, the program will expect the name of a pipeline and will run the entire pipeline. Each processor in the pipeline will perform any work that needs to be done (in order) until there is no more work left.

```bash
sbt "run --pipeline <pipeline>"
```

_Again, remember `--reprocess` and `--yes` when applicable._

## Configuration Loading

The default configuration parameter is `config.json`, but can be overridden with `--config <file>` on the command line. This is a sample configuration file:

```json
{
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
      "key": "SG.xxx",
      "from": "do-not-reply@broadinstitute.org",
      "emails": [
        "me@broadinstitute.org",
        "someone@broadinstitute.org"
      ]
    }
}
```

This is where all "private" configuration settings should be kept, and **not** committed to the repository.

## Packages

The root package is `org.broadinstitute.dig.aggregator`. Within that package are the following:

### app

Contains the `Main` entry point and any "application specific" code.

### core

The heart of the repository and contains all the shared code that is used by all the pipelines and processors to do their work.

### pipeline

Each sub-package is a pipeline, which is broken up into its various processors.

## Resources

The `src/main/resources/pipeline` folder contains all the job scripts (each in the appropriate pipeline folder parallel to `org.broadinstitute.dig.aggregator.pipeline._`) used by the various processors. These scripts are uploaded to S3 so all nodes on the EMR cluster can load and execute them.

## Processor Classes

There are a few different type of processors that are implemented in each pipeline:

### IntakeProcessor

The `IntakeProcessor` is strictly for use within the `intake` pipeline. This is a processor that reads from Kafka and does processes each record.

### UploadProcessor

The `UploadProcessor` is a specific type of `IntakeProcessor`. It assumes that the Kafka topic being consumed is uploading datasets and adheres to a very specific protocol: CREATE, UPLOAD, and COMMIT. Each message will be JSON and will contain the unique ID of the dataset, a method (CREATE, UPLOAD, or COMMIT), and a body that is unique per method.

The first record for a dataset on the topic will be the `COMMIT` message:

```json
{
  "id": "unique_dataset_id",
  "method": "COMMIT",
  "body": {
    // metadata for this dataset
  }
}
```

When this message is encountered, any existing data in S3 for this dataset is deleted and a new directory is created with a single `metadata` file containing the contents of `body`. This is done per-topic. For example, the `variants` topic would end up writing to:

```
s3://dig-analysis-data/variants/unique_dataset_id/metadata
```

Next, zero or more `UPLOAD` messages are sent:

```json
{
  "id": "unique_dataset_id",
  "method": "UPLOAD",
  "body": {
    "count": 100,
    "data": [
      {
        // a single entry for this dataset
      },

      // ...(count-1) more items...
    ]
  }
}
```

These messages are read by the `UploadProcessor` and the entries are written to a single file in the directory for the dataset. Each line of the file is a single JSON object intended to be processed by Spark:

```
{entry-1}
{entry-2}
{entry-3}
...
```

The filename given to each is `data-<offset>`, where `<offset>` is the partition offset this record is at, guaranteeing uniqueness across all files in the dataset.

Finally, a `COMMIT` message is sent (with no `body`). This signals that the dataset is done and can now be processed further by other processors in across all pipelines.

### JobProcessor

A `JobProcessor` is simply a processor that has resources that need to be uploaded to S3 (e.g. PySpark scripts) before the processor can run.

### DatasetProcessor

Once a dataset has been completely uploaded and committed to HDFS (S3), it can then be processed by a `DatasetProcessor` (a subclass of `JobProcessor`). Dataset processors look for any new datasets committed to a given topic and take the next step necessary to prepare it for future use. 

DatasetProcessors are (typically) the first processor in a pipeline to run as they have no dependencies other than a new dataset being uploaded.

### RunProcessor

A `RunProcessor` is a `JobProcessor` that has other processors as dependencies. It waits until a dependency processor has new output, then uses that output as input for its own process. After a `DatasetProcessor` executes, run processors typically make up the rest of the process "graph" for a pipeline.

## Processor Database

Each processor knows how to save its current state and how to either pick up where it left off or check for new work to be processed.

An `IntakeProcessor` uses the `offsets` table in the database. It tracks what offsets (for each partition on a given topic) have already been successfully processed. A `RunProcessor` uses the `runs` table keeps track of what inputs have been processed by each processor already. Consider the following example:

> The `VariantProcessor` (an `UploadProcessor`) reads from the topic `varaints` in Kafka and uploads datasets to S3. As it consumes messages, it writes to the `offsets` table each offset consumed for each partition.
>
> Once the `VariantProcessor` receives the `COMMIT` message, it sends that message back to the `commits` topic in Kafka, and the `CommitProcessor` (an `IntakeProcessor`) gets it and writes it to the database.
>
> At this point, the `metaanalysis.VariantPartitionProcessor` (a `DatasetProcessor`) can see in the `commits` table that a dataset has been added that it hasn't yet processed (there is no `runs` entry for it, it the existing entry for that dataset is out of date as the dataset has been updated). It then runs, using the dataset as input and producing an output (in this example, a phenotype) and writes the run to the database.
>
> Next, the `metaanalysis.AncestrySpecificProcessor`, which depends on the variant partition processor, discovers that one of its dependencies has produced a new output that it hasn't yet processed, and starts to run.
>
> This continues until the entire meta-analysis pipeline is complete...

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
