package org.broadinstitute.dig.aggregator.core.processors.loamstream

import org.scalatest.FunSuite
import loamstream.conf.LoamConfig
import com.typesafe.config.ConfigFactory
import java.nio.file.Path
import java.util.UUID
import java.nio.file.Paths
import java.nio.file.Files.exists
import org.apache.commons.io.FileUtils
import loamstream.util.Files
import org.broadinstitute.dig.aggregator.core.config.BaseConfig
import org.broadinstitute.dig.aggregator.core.Processor
import loamstream.apps.LoamRunner
import loamstream.compiler.LoamEngine
import java.net.URI
import org.broadinstitute.dig.aggregator.core.Run
import org.broadinstitute.dig.aws.config.AWSConfig
import org.broadinstitute.dig.aws.config.S3Config
import org.broadinstitute.dig.aws.config.emr.EmrConfig
import org.broadinstitute.dig.aws.config.emr.SubnetId
import org.broadinstitute.dig.aggregator.core.config.MySQLConfig
import org.broadinstitute.dig.aggregator.core.config.Neo4jConfig

/**
 * @author clint
 * Nov 7, 2019
 */
final class SimpleLoamPipelineTest extends FunSuite {
  test("Run a processor that runs a simple Loam script") {
    //Just defaults
    val loamConfig = LoamConfig.fromConfig(ConfigFactory.parseString("loamstream { }")).get
    
    import loamstream.util.Paths.Implicits._
    
    withWorkDir() { workDir =>
      
      val aPath = workDir / "a.txt"
      val bPath = workDir / "b.txt"
      
      val loamFile = workDir / s"${this.getClass.getSimpleName}.loam"
      
      val loamCode = s"""
      import org.broadinstitute.dig.aggregator.core.processors.loamstream.AggregatorSupport

      val processorName = AggregatorSupport.processorContext.name

val a = store("${aPath}").asInput
val b = store("${bPath}")

cmd"cp $${a} $${b}".in(a).out(b).tag(processorName)

"""
      Files.writeTo(aPath)("ASDF")
      Files.writeTo(loamFile)(loamCode)
      
      assert(exists(aPath))
      assert(exists(bPath) === false)
      assert(exists(loamFile))
    
      def ctor(name: Processor.Name, baseConfig: BaseConfig): LoamProcessor = new LoamProcessor(
        name, 
        baseConfig,
        loamConfig,
        LoamRunner(LoamEngine.default(loamConfig)),
        Seq(loamFile)) { 
        
        override protected def resourcesByName: Map[String, URI] = Map.empty
        
        override val dependencies: Seq[Processor.Name] = Nil
        
        override def getOutputs(input: Run.Result): Processor.OutputList = Processor.NoOutputs
      }
      
      val name = Processor.register(s"${this.getClass.getSimpleName}Processor", ctor)
      
      val baseConfig = BaseConfig(
        aws = AWSConfig(s3 = S3Config("fooBucket"), emr = EmrConfig(sshKeyName = "lalala", subnetId = SubnetId("subnet-blarg"))),
        mysql = MySQLConfig(host = "no", port = -1, engine = "mysql", username = "no", password = "no"),
        neo4j = Neo4jConfig(host = "no", port = -1, username = "no", password = "no"))
        
      val processor = ctor(name, baseConfig)
      
      assert(exists(aPath))
      assert(exists(bPath) === false)
      
      //HACK ALERT
      Paths.get(".loamstream/logs/").toFile.mkdirs()
      
      processor.processOutputs(Nil).unsafeRunSync()
      
      assert(exists(aPath))
      assert(exists(bPath))
      assert(Files.readFrom(aPath) === Files.readFrom(bPath))
    }
  }
  
  private def withWorkDir[A](baseName: String = getClass.getSimpleName)(body: Path => A): A = {
    val workDir = Paths.get("target", s"${baseName}-${UUID.randomUUID}")
    
    assert(exists(workDir) === false)
    
    workDir.toFile.mkdirs()
    
    assert(exists(workDir))
    
    try { body(workDir) }
    finally { FileUtils.deleteQuietly(workDir.toFile) ; () }
  }
}
