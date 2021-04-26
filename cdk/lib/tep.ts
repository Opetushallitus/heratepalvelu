import cdk = require("@aws-cdk/core");
import dynamodb = require("@aws-cdk/aws-dynamodb");
import events = require("@aws-cdk/aws-events");
import targets = require("@aws-cdk/aws-events-targets");
import lambda = require("@aws-cdk/aws-lambda");
import s3assets = require("@aws-cdk/aws-s3-assets");
import sqs = require("@aws-cdk/aws-sqs");
import iam = require("@aws-cdk/aws-iam");
import { SqsEventSource } from "@aws-cdk/aws-lambda-event-sources";
import { Duration, Token } from "@aws-cdk/core";
import { HeratepalveluStack } from "./heratepalvelu";
import {CfnEventSourceMapping} from "@aws-cdk/aws-lambda";


export class HeratepalveluTEPStack extends HeratepalveluStack {
  constructor(
    scope: cdk.App,
    id: string,
    envName: string,
    version: string,
    props?: cdk.StackProps
  ) {
    super(scope, id, envName, version, props);

    // DynamoDB

    const jaksotunnusTable = new dynamodb.Table(this, "jaksotunnusTable", {
      partitionKey: {
        name: "hankkimistapa_id",
        type: dynamodb.AttributeType.NUMBER
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      serverSideEncryption: true
    });

    jaksotunnusTable.addGlobalSecondaryIndex({
      indexName: "niputusIndex",
      partitionKey: {
        name: "ohjaaja_ytunnus_kj_tutkinto",
        type: dynamodb.AttributeType.STRING
      },
      sortKey: {
        name: "niputuspvm",
        type: dynamodb.AttributeType.STRING
      },
      nonKeyAttributes: [
        "tunnus",
        "oppilaitos",
        "tyopaikan_nimi"
      ],
      projectionType: dynamodb.ProjectionType.INCLUDE
    });

    const nippuTable = new dynamodb.Table(this, "nippuTable", {
      partitionKey: {
        name: "ohjaaja_ytunnus_kj_tutkinto",
        type: dynamodb.AttributeType.STRING
      },
      sortKey: {
        name: "niputuspvm",
        type: dynamodb.AttributeType.STRING
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      serverSideEncryption: true
    });

    // nippuTable.addGlobalSecondaryIndex({
    //   indexName: "niputusIndex",
    //   partitionKey: {
    //     name: "kasittelytila",
    //     type: dynamodb.AttributeType.STRING
    //   },
    //   sortKey: {
    //     name: "niputuspvm",
    //     type: dynamodb.AttributeType.STRING
    //   },
    //   nonKeyAttributes: [
    //     "ohjaaja_ytunnus_kj_tutkinto",
    //     "koulutuksenjarjestaja",
    //     "tutkinto"
    //   ],
    //   projectionType: dynamodb.ProjectionType.INCLUDE
    // });

    const organisaatioWhitelistTable = new dynamodb.Table(
        this,
        "OrganisaatioWhitelistTable",
        {
          partitionKey: {
            name: "organisaatio-oid",
            type: dynamodb.AttributeType.STRING
          },
          billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
          serverSideEncryption: true
        }
    );

    // SQS

    const herateDeadLetterQueue = new sqs.Queue(this, "HerateDLQ", {
      retentionPeriod: Duration.days(14),
      visibilityTimeout: (Duration.seconds(60))
    });

    const herateQueue = new sqs.Queue(this, "HerateQueue", {
      queueName: `${id}-HerateQueue`,
      deadLetterQueue: {
        queue: herateDeadLetterQueue,
        maxReceiveCount: 5
      },
      visibilityTimeout: Duration.seconds(60),
      retentionPeriod: Duration.days(14)
    });

    // S3

    const ehoksHerateTEPAsset = new s3assets.Asset(
      this,
      "EhoksHerateTEPLambdaAsset",
      {
        path: "../target/uberjar/heratepalvelu-0.1.0-SNAPSHOT-standalone.jar"
      }
    );

    // Lambda

    const lambdaCode = lambda.Code.fromBucket(
        ehoksHerateTEPAsset.bucket,
        ehoksHerateTEPAsset.s3ObjectKey
    );

    // herateHandler

    const timedOperationsHandler = new lambda.Function(this, "timedOperationsHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        caller_id: `1.2.246.562.10.00000000001.${id}-timedOperationsHandler`,
      },
      memorySize: Token.asNumber(1024),
      timeout: Duration.seconds(900),
      handler: "oph.heratepalvelu.tep.ehoksTimedOperationsHandler::handleTimedOperations",
      tracing: lambda.Tracing.ACTIVE
    });

    new events.Rule(this, "TimedOperationsScheduleRule", {
      schedule: events.Schedule.expression(
          `rate(${this.getParameterFromSsm("timedoperations-rate")})`
      ),
      targets: [new targets.LambdaFunction(timedOperationsHandler)]
    });

    // jaksoHandler

    const jaksoHandler = new lambda.Function(this, "TEPJaksoHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        jaksotunnus_table: jaksotunnusTable.tableName,
        nippu_table: nippuTable.tableName,
        orgwhitelist_table: organisaatioWhitelistTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-JaksoHandler`,
      },
      handler: "oph.heratepalvelu.tep.jaksoHandler::handleJaksoHerate",
      memorySize: Token.asNumber(this.getParameterFromSsm("jaksohandler-memory")),
      reservedConcurrentExecutions:
          Token.asNumber(this.getParameterFromSsm("jaksohandler-concurrency")),
      timeout: Duration.seconds(
          Token.asNumber(this.getParameterFromSsm("jaksohandler-timeout"))
      ),
      tracing: lambda.Tracing.ACTIVE
    });

    jaksoHandler.addEventSource(new SqsEventSource(herateQueue, { batchSize: 1 }));
    jaksotunnusTable.grantReadWriteData(jaksoHandler);
    nippuTable.grantReadWriteData(jaksoHandler);
    organisaatioWhitelistTable.grantReadData(jaksoHandler);

    // niputusHandler

    const niputusHandler = new lambda.Function(this, "niputusHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        jaksotunnus_table: jaksotunnusTable.tableName,
        nippu_table: nippuTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-niputusHandler`,
      },
      memorySize: Token.asNumber(1024),
      timeout: Duration.seconds(900),
      handler: "oph.heratepalvelu.tep.niputusHandler::handleNiputus",
      tracing: lambda.Tracing.ACTIVE
    });

    new events.Rule(this, "niputusHandlerScheduleRule", {
      schedule: events.Schedule.expression(
        `rate(20 minutes)`
      ),
      targets: [new targets.LambdaFunction(niputusHandler)]
    });

    jaksotunnusTable.grantReadData(niputusHandler);
    nippuTable.grantReadWriteData(niputusHandler);

    // emailHandler

    // DLQ tyhjennys

    const dlqResendHandler = new lambda.Function(this, "TEP-DLQresendHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        queue_name: herateQueue.queueName
      },
      handler: "oph.heratepalvelu.DLQresendHandler::handleDLQresend",
      memorySize: 1024,
      timeout: Duration.seconds(60),
      tracing: lambda.Tracing.ACTIVE
    });

    dlqResendHandler.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [herateQueue.queueArn, herateDeadLetterQueue.queueArn],
      actions: [
        "sqs:GetQueueUrl",
        "sqs:SendMessage",
        "sqs:ReceiveMessage",
        "sqs:ChangeMessageVisibility",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ]}));

    new CfnEventSourceMapping(this, "DLQResendEventSourceMapping", {
      eventSourceArn: herateDeadLetterQueue.queueArn,
      functionName: dlqResendHandler.functionName,
      batchSize: 1,
      enabled: false
    });

    // const dbUpdateHandler = new lambda.Function(this, "dbUpdateHandler", {
    //   runtime: lambda.Runtime.JAVA_8,
    //   code: lambdaCode,
    //   environment: {
    //     nippu_table: nippuTable.tableName
    //   },
    //   memorySize: Token.asNumber(1024),
    //   timeout: Duration.seconds(900),
    //   handler: "oph.heratepalvelu.tep.dbChanger::handleDBUpdate",
    //   tracing: lambda.Tracing.ACTIVE
    // });
    // nippuTable.grantReadWriteData(dbUpdateHandler);
    //
    // new events.Rule(this, "dbUpdateHandlerScheduleRule", {
    //   schedule: events.Schedule.expression(
    //     `rate(20 minutes)`
    //   ),
    //   targets: [new targets.LambdaFunction(dbUpdateHandler)]
    // });

    // IAM

    [jaksoHandler, timedOperationsHandler, niputusHandler].forEach(
        lambdaFunction => {
          lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            resources: [`arn:aws:ssm:eu-west-1:*:parameter/${envName}/services/heratepalvelu/*`],
            actions: ['ssm:GetParameter']
          }));
        }
    );
  }
}
