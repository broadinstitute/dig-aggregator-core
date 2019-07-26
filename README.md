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

When testing changes to processors, it can be quite a pain to run over _everything_. You can pass `--only` along with an input parameter which is a comma-separated list of globs to limit what inputs actually get processed to a single one for testing purposes:

```bash
sbt "run --yes --only T2D,BMI,CKD* MetaAnalysisProcessor"
```

Similarly, you can use the `--exclude` flag to remove something that would otherwise be processed from the list:

```bash
sbt "run --yes --exclude T2D,CKD* MetaAnalysisProcessor"
```

_You can use `--only` and `--exclude` together as well._

## Running Pipelines

If `--pipeline` is present on the command line, then instead of a `<processor>` name, the program will expect the name of a pipeline and will run the entire pipeline. Each processor in the pipeline will perform any work that needs to be done (in order) until there is no more work left.

```bash
sbt "run --pipeline <pipeline>"
```

_Again, remember `--reprocess` and `--yes` when applicable._

## AWS Secrets

The aggregator makes heavy use of AWS and AWS Secrets for connection settings. Please see [https://github.com/broadinstitute/dig-secrets][secrets] for details on setting up your environment to use AWS and accessing secrets.

## Configuration Loading

The default configuration parameter is `config.json`, but can be overridden with `--config <file>` on the command line. This is a sample configuration file:

```json
{
    "aws": {
        "s3": {
            "bucket": "s3-bucket-name"
        },
        "emr": {
            "subnetId": "subnet-xxx",
            "sshKeyName": "AWS SSH key used to connect to EMR instances",
            "securityGroupIds": [
                "sg-xxx"
            ]
        }
    },
    "neo4jSecretId": "secret-id",
    "mysqlSecretId":  "secret-id"
}
```

None of the settings stored are considered to be "secret", but the configuration file is unique to the group running the aggregator and so should not be committed to version control.

The `"neo4jSecretId"` and `"mysqlSecretId"` properties are the AWS secret IDs used to access the host, port, and user credentials needed to connect to both the graph and runs databases.

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

## Processor Class

Every processor has both resources that are uploaded to S3 (used for running jobs) and dependencies: the outputs of other processors used as inputs. Processors _must_ implement a few methods so that the aggregator knows how to run them and what to commit to the runs database once it has run successfully.

## Runs Database

Each processor knows how to save its current state and how to either pick up where it left off or check for new work to be processed.

# fin.

[scala]: https://scala-lang.org/
[io]: https://typelevel.org/cats-effect/datatypes/io.html
[aws]: https://aws.amazon.com/
[kafka]: https://kafka.apache.org/
[hadoop]: https://hadoop.apache.org/
[crud]: https://en.wikipedia.org/wiki/Create,_read,_update_and_delete
[mr]: https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html
[secrets]: https://github.com/broadinstitute/dig-secrets
[s3]: https://aws.amazon.com/s3/
[emr]: https://aws.amazon.com/emr/
[mysql]: https://www.mysql.com/
[doobie]: https://tpolecat.github.io/doobie/
[spark]: http://spark.apache.org/
[pig]: http://pig.apache.org/
[mr]: https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
[neo4j]: https://neo4j.com
