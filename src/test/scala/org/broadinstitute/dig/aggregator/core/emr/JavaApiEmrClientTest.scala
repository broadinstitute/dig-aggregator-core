package org.broadinstitute.dig.aggregator.core.emr

import org.scalatest.FunSuite
import java.net.URI
import scala.collection.JavaConverters._

/**
 * @author clint
 * Oct 24, 2018
 */
final class JavaApiEmrClientTest extends FunSuite {
  test("makeRequest") {
    import JavaApiEmrClient.makeClusterCreationRequest
    
    def doTest(amiId: Option[AmiId]): Unit = {
      val clusterName = "foo"
      val applications: Seq[ApplicationName] = Seq(ApplicationName.Hadoop, ApplicationName.Pig)
      val instances = 42
      val releaseLabel = EmrReleaseId("emr-foo")
      val serviceRole = RoleId("a")
      val jobFlowRole = RoleId("b")
      val autoScalingRole = RoleId("c")
      val visibleToAllUsers = true
      val sshKeyId = SshKeyId("d")
      val keepJobFlowAliveWhenNoSteps = true
      val masterInstanceType = InstanceType.m4.xlarge
      val slaveInstanceType = InstanceType.m3.xlarge
      val bootstrapScripts = Seq(URI.create("https://example.com/x"), URI.create("https://example.com/y"))
      val securityGroupIds = Seq(SecurityGroupId("e"), SecurityGroupId("f"))
      val subnetId = SubnetId("g")
      val logUri = URI.create("https://example.com/y")
      
      val req = makeClusterCreationRequest(
        clusterName = clusterName,
        applications = applications,
        instances = instances,
        releaseLabel = releaseLabel,
        serviceRole = serviceRole,
        jobFlowRole = jobFlowRole,
        autoScalingRole = autoScalingRole,
        visibleToAllUsers = visibleToAllUsers,
        sshKeyName = sshKeyId,
        keepJobFlowAliveWhenNoSteps = keepJobFlowAliveWhenNoSteps,
        masterInstanceType = masterInstanceType, 
        slaveInstanceType = slaveInstanceType,
        bootstrapScripts = bootstrapScripts,
        securityGroupIds = securityGroupIds,
        subnetId = subnetId,
        logUri = logUri,
        amiId = amiId)
        
      assert(req.getName === clusterName)
      
      assert(req.getApplications.asScala.map(_.getName).toSet === Set(ApplicationName.Hadoop.name, ApplicationName.Pig.name))
      assert(req.getApplications.asScala.forall(_.getArgs.isEmpty))
      assert(req.getApplications.asScala.forall(_.getAdditionalInfo.isEmpty))
      assert(req.getApplications.asScala.forall(_.getVersion === null))
      
      assert(req.getInstances.getInstanceCount.intValue === instances)
      
      assert(req.getReleaseLabel === releaseLabel.value)
      
      assert(req.getServiceRole === serviceRole.value)
      
      assert(req.getJobFlowRole === jobFlowRole.value)
      
      assert(req.getAutoScalingRole === autoScalingRole.value)
      
      assert(req.getVisibleToAllUsers === visibleToAllUsers)
      
      assert(req.getInstances.getEc2KeyName === sshKeyId.value)
      
      assert(req.getInstances.getKeepJobFlowAliveWhenNoSteps === keepJobFlowAliveWhenNoSteps)
      
      assert(req.getInstances.getMasterInstanceType === masterInstanceType.value)
      
      assert(req.getInstances.getSlaveInstanceType === slaveInstanceType.value)
      
      assert(req.getBootstrapActions.asScala.map(_.getName) === Seq("https://example.com/x", "https://example.com/y"))
      assert(req.getBootstrapActions.asScala.map(_.getScriptBootstrapAction.getArgs.asScala) === Seq(Nil, Nil))
      assert(req.getBootstrapActions.asScala.map(_.getScriptBootstrapAction.getPath) === 
          Seq("https://example.com/x", "https://example.com/y"))
          
      assert(req.getInstances.getAdditionalMasterSecurityGroups.asScala === securityGroupIds.map(_.value))
      assert(req.getInstances.getAdditionalSlaveSecurityGroups.asScala === securityGroupIds.map(_.value))
      
      assert(req.getInstances.getEc2SubnetId === subnetId.value)
      
      assert(req.getLogUri === logUri.toString)
      
      amiId match {
        case None => assert(req.getCustomAmiId === null)
        case Some(id) => assert(req.getCustomAmiId === id.value)
      }
    }
    
    doTest(None)
    doTest(Some(AmiId("bar")))
  }
  
  test("isSuccess") {
    import JavaApiEmrClient.isSuccess
    
    assert(isSuccess(200) === true)
    
    assert(isSuccess(0) === false)
    assert(isSuccess(201) === false)
    assert(isSuccess(199) === false)
    assert(isSuccess(-10) === false)
    assert(isSuccess(300) === false)
  }
}
