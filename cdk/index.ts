import { App, Stack, RemovalPolicy } from '@aws-cdk/core'
import { CfnCluster, CfnClusterSubnetGroup } from '@aws-cdk/aws-redshift'
import { Vpc, SecurityGroup, Port, SubnetType } from '@aws-cdk/aws-ec2'
import { Role, ServicePrincipal } from '@aws-cdk/aws-iam'
import { Bucket, BlockPublicAccess } from '@aws-cdk/aws-s3'
import * as dotenv from 'dotenv'

dotenv.config()

export class RedPandaTestStack extends Stack {
  constructor(scope: App, id: string) {
    super(scope, id)

    const role = new Role(this, 'RedPandaTestRole', {
      assumedBy: new ServicePrincipal('redshift.amazonaws.com')
    })

    const bucket = new Bucket(this, 'RedPandaTestBucket', {
      versioned: false,
      publicReadAccess: false,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: RemovalPolicy.DESTROY
    })

    bucket.grantReadWrite(role)

    const vpc = new Vpc(this, 'RedPandaTestVPC', {})

    const sg = new SecurityGroup(this, 'RedPandaTestSecurityGroup', {
      vpc: vpc
    })

    sg.connections.allowFromAnyIpv4(Port.tcp(5439))
    sg.node.addDependency(vpc)

    const { subnetIds } = vpc.selectSubnets({ subnetType: SubnetType.PUBLIC })
    const subnetGroup = new CfnClusterSubnetGroup(this, 'RedPandaTestSubnets', {
      description: `Subnets for Redshift cluster`,
      subnetIds
    })

    subnetGroup.applyRemovalPolicy(RemovalPolicy.RETAIN, {
      applyToUpdateReplacePolicy: true
    })

    const cluster = new CfnCluster(this, 'RedPandaTestRedshift', {
      masterUsername: process.env.REDSHIFT_USERNAME || '',
      masterUserPassword: process.env.REDSHIFT_PASSWORD || '',
      dbName: process.env.REDSHIFT_DB || '',
      clusterType: 'single-node',
      port: parseInt(process.env.REDSHIFT_PORT || '5439'),
      nodeType: 'dc2.large',
      iamRoles: [role.roleArn],
      publiclyAccessible: true,
      vpcSecurityGroupIds: [sg.securityGroupId],
      clusterSubnetGroupName: subnetGroup.ref
    })

    cluster.node.addDependency(sg)
  }
}

const app = new App()
new RedPandaTestStack(app, 'RedPandaTestStack')
app.synth()
