import { App, Stack, RemovalPolicy } from '@aws-cdk/core'
import { CfnCluster, CfnClusterSubnetGroup } from '@aws-cdk/aws-redshift'
import { Vpc, SecurityGroup, Port, SubnetType } from '@aws-cdk/aws-ec2'
import { Role, ServicePrincipal } from '@aws-cdk/aws-iam'
import { Bucket, BlockPublicAccess } from '@aws-cdk/aws-s3'
import { Database, Table, Schema, DataFormat } from '@aws-cdk/aws-glue'
import { v4 as uuidv4 } from 'uuid'
import * as dotenv from 'dotenv'

dotenv.config({ path: '../.env' })

export class RedPandaTestStack extends Stack {
  constructor(scope: App, id: string) {
    super(scope, id)

    // Bucket
    const role = new Role(this, 'RedPandaTestRole', {
      assumedBy: new ServicePrincipal('redshift.amazonaws.com')
    })

    const bucket = new Bucket(this, 'RedPandaTestBaseBucket', {
      bucketName: `redpandateststack-base-${uuidv4()}`,
      versioned: false,
      publicReadAccess: false,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: RemovalPolicy.DESTROY
    })

    bucket.grantReadWrite(role)

    // Redshift
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

    // Glue
    const glueDB = new Database(this, 'RedPandaTestGlueDB', {
      databaseName: 'redpandatestgluedb'
    })

    const glueBucket = new Bucket(this, 'RedPandaTestGlueBucket', {
      bucketName: `redpandateststack-glue-${uuidv4()}`,
      versioned: false,
      publicReadAccess: false,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: RemovalPolicy.DESTROY
    })

    new Table(this, 'MyTable', {
      bucket: glueBucket,
      database: glueDB,
      tableName: 'redpandatestgluetable',
      columns: [
        {
          name: 'col0',
          type: Schema.STRING
        },
        {
          name: 'col1',
          type: Schema.STRING
        }
      ],
      dataFormat: DataFormat.CSV
    })

    // Athena
    new Bucket(this, 'RedPandaTestAthenaBucket', {
      bucketName: `redpandateststack-athena-${uuidv4()}`,
      versioned: false,
      publicReadAccess: false,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: RemovalPolicy.DESTROY
    })
  }
}

const app = new App()
new RedPandaTestStack(app, 'RedPandaTestStack')
app.synth()
