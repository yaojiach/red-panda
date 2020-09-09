import { App, Stack, RemovalPolicy } from '@aws-cdk/core'
import { CfnCluster, CfnClusterSubnetGroup } from '@aws-cdk/aws-redshift'
import { Vpc, SecurityGroup, Port, SubnetType } from '@aws-cdk/aws-ec2'
import { Role, ServicePrincipal } from '@aws-cdk/aws-iam'
import { Bucket, BlockPublicAccess } from '@aws-cdk/aws-s3'
import { Database, Table, Schema, DataFormat } from '@aws-cdk/aws-glue'
import { v4 as uuidv4 } from 'uuid'
import * as dotenv from 'dotenv'

dotenv.config({ path: '../.env' })

const ID_PREFIX = 'RedPandaTest'
const STACK_NAME = process.env.STACK_NAME

const makeId = (suffix: string) => `${ID_PREFIX}${suffix}`

export class RedPandaTestStack extends Stack {
  constructor(scope: App, id: string) {
    super(scope, id)

    // Bucket
    const role = new Role(this, makeId('Role'), {
      assumedBy: new ServicePrincipal('redshift.amazonaws.com')
    })

    const bucket = new Bucket(this, makeId('BaseBucket'), {
      bucketName: `${STACK_NAME}-${process.env.BASE_BUCKET_ID}-${uuidv4()}`,
      versioned: false,
      publicReadAccess: false,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: RemovalPolicy.DESTROY
    })

    bucket.grantReadWrite(role)

    // Redshift
    const vpc = new Vpc(this, makeId('VPC'), {})

    const sg = new SecurityGroup(this, makeId('SecurityGroup'), {
      vpc: vpc
    })

    sg.connections.allowFromAnyIpv4(Port.tcp(5439))
    sg.node.addDependency(vpc)

    const { subnetIds } = vpc.selectSubnets({ subnetType: SubnetType.PUBLIC })
    const subnetGroup = new CfnClusterSubnetGroup(this, makeId('Subnets'), {
      description: 'Subnets for Redshift cluster',
      subnetIds
    })

    subnetGroup.applyRemovalPolicy(RemovalPolicy.RETAIN, {
      applyToUpdateReplacePolicy: true
    })

    const cluster = new CfnCluster(this, makeId('Redshift'), {
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
    const glueDB = new Database(this, makeId('GlueDB'), {
      databaseName: process.env.GLUE_DB || ''
    })

    const glueBucket = new Bucket(this, makeId('GlueBucket'), {
      bucketName: `${STACK_NAME}-${process.env.GLUE_BUCKET_ID}-${uuidv4()}`,
      versioned: false,
      publicReadAccess: false,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: RemovalPolicy.DESTROY
    })

    new Table(this, makeId('GlueTable'), {
      bucket: glueBucket,
      database: glueDB,
      tableName: process.env.GLUE_TABLE_NAME || '',
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
    new Bucket(this, makeId('AthenaBucket'), {
      bucketName: `${STACK_NAME}-${process.env.ATHENA_BUCKET_ID}-${uuidv4()}`,
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
