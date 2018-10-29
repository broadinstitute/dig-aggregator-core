package org.broadinstitute.dig.aggregator.core.emr

import scala.collection.JavaConverters._
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import org.broadinstitute.dig.aggregator.app.Opts
import org.apache.commons.io.IOUtils
import org.broadinstitute.dig.aggregator.core.AWS
import scala.collection.Seq
import java.net.URI
import cats.effect.IO

/**
 * @author clint
 * Oct 4, 2018
 */
object ClusterAutomation extends App {
  
  private val opts: Opts = new Opts(Array("--config", "src/it/resources/config.json"))
    
  private val credentials: AWSStaticCredentialsProvider = {
    new AWSStaticCredentialsProvider(new BasicAWSCredentials(opts.config.aws.key, opts.config.aws.secret))
  }
    
  private val aws: AWS = new AWS(opts.config.aws)
  
  run()
  
  def run(): Unit = {
    val helloSparkContents: String = """
      |from pyspark import SparkContext
      |from operator import add
      |
      |sc = SparkContext()
      |data = sc.parallelize(list("Hello World"))
      |counts = data.map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[1], ascending=False).collect()
      |for (word, count) in counts:
      |    print("{}: {}".format(word, count))
      |sc.stop()
      |""".stripMargin.trim
    
    //"emr/cluster-bootstrap.sh"
      
    def getResourceAsString(name: String): String = {
      val stream = getClass.getClassLoader.getResourceAsStream(name)
      
      require(stream != null)
      try {
        IOUtils.toString(stream)
      } finally {
        stream.close()
      }
    }
    
    def uploadBootstrapScript(name: String): IO[URI] = {
      aws.put(name, getResourceAsString(s"emr/${name}")).map(_ => aws.uriOf(name))
    }
    
    val client: EmrClient = new JavaApiEmrClient(aws)
    
    val io = for {
      _ <- aws.put("hello-spark.py", helloSparkContents)
      clusterBootstrapInstallDeps <- uploadBootstrapScript("cluster-bootstrap-install-deps.sh")
      clusterBootstrap <- uploadBootstrapScript("cluster-bootstrap.sh")
      id <- client.createCluster(
        instances = 3,
        bootstrapScripts = Seq(clusterBootstrapInstallDeps, clusterBootstrap),
        masterInstanceType = InstanceType.m4.xlarge,
        slaveInstanceType = InstanceType.m4.xlarge,
        //amiId = Some(AmiId.amazonLinux2018Dot03))
        amiId = Some(AmiId("ami-05d585056c5a2c2b7")))
      _ = println(s"Made request, job flow id = '${id}'")
      _ <- client.runOnCluster(id, aws.uriOf("hello-spark.py"))
      clusters <- client.listClusters
      _ = println(s"${clusters.size} clusters:")
      _ = clusters.foreach(println)
    } yield {
      ()
    }
    
    io.unsafeRunSync()
  }
}
