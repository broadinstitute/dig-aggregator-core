package org.broadinstitute.dig.aggregator.core.emr

/**
 * @author clint
 * Oct 24, 2018
 */
final case class AmiId(value: String)

object AmiId {
  //
  //Stock Amazon Linux 2018.03 Linux AMI with the non-problematic creation date.
  //Note that Amazon Linux 2 AMIs can't be used for EMR cluster nodes.
  val amazonLinux2018Dot03: AmiId = AmiId("ami-f316478c")
  
  //
  //Custom AMI based on amazonLinux2018Dot03 with some packages installed:
  //Yum: amazon-efs-utils
  //Pip: boto 2.39.0, neo4j-driver 1.6.1, scipy 1.1.0
  //METAL (in /opt/generic-metal)
  //LDSC (in /opt/ldsc)
  val customAmiWithMetalAndLdsc: AmiId = AmiId("ami-0ac766786a5a04a14")
}
