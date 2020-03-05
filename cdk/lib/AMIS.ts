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
import { CfnEventSourceMapping } from "@aws-cdk/aws-lambda";
import { HeratepalveluStack } from "./heratepalvelu";

export class HeratepalveluAMISStack extends HeratepalveluStack {
  constructor(
    scope: cdk.Construct,
    id: string,
    envName: string,
    version: string,
    props?: cdk.StackProps
  ) {
    super(scope, id, envName, version, props);

    /////////////////
    // DynamoDB
    /////////////////

    const AMISherateTable = new dynamodb.Table(this, "AMISHerateTable", {
      partitionKey: {
        name: "toimija_oppija",
        type: dynamodb.AttributeType.STRING
      },
      sortKey: {
        name: "tyyppi_kausi",
        type: dynamodb.AttributeType.STRING
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      serverSideEncryption: true
    });

    AMISherateTable.addGlobalSecondaryIndex({
      indexName: "lahetysIndex",
      partitionKey: {
        name: "lahetystila",
        type: dynamodb.AttributeType.STRING
      },
      sortKey: {
        name: "alkupvm",
        type: dynamodb.AttributeType.STRING
      },
      nonKeyAttributes: [
        "sahkoposti",
        "kyselylinkki",
        "suorituskieli",
        "viestintapalvelu-id",
        "kyselytyyppi"
      ],
      projectionType: dynamodb.ProjectionType.INCLUDE
    });

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

    const metadataTable = new dynamodb.Table(this, "MetadataTable", {
      partitionKey: {
        name: "key",
        type: dynamodb.AttributeType.STRING
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      serverSideEncryption: true
    });

    /////////////////
    // SQS
    /////////////////

    const herateDeadLetterQueue = new sqs.Queue(this, "HerateDeadLetterQueue", {
      retentionPeriod: Duration.days(14)
    });

    const ehoksHerateQueue = new sqs.Queue(this, "HerateQueue", {
      queueName: `${id}-eHOKSHerateQueue`,
      deadLetterQueue: {
        queue: herateDeadLetterQueue,
        maxReceiveCount: 5
      },
      visibilityTimeout: Duration.seconds(60),
      retentionPeriod: Duration.days(14)
    });

    /////////////////
    // S3
    /////////////////

    const ehoksHerateAsset = new s3assets.Asset(
      this,
      "EhoksHerateAMISLambdaAsset",
      {
        path: "../target/uberjar/heratepalvelu-0.1.0-SNAPSHOT-standalone.jar"
      }
    );

    /////////////////
    // Lambda
    /////////////////

    const lambdaCode = lambda.Code.fromBucket(
      ehoksHerateAsset.bucket,
      ehoksHerateAsset.s3ObjectKey
    );

    const AMISHerateHandler = new lambda.Function(this, "AMISHerateHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        herate_table: AMISherateTable.tableName,
        orgwhitelist_table: organisaatioWhitelistTable.tableName,
        caller_id: `${id}-AMISherateHandler`,
      },
      handler: "oph.heratepalvelu.AMISherateHandler::handleAMISherate",
      memorySize: Token.asNumber(this.getParameterFromSsm("ehokshandler-memory")),
      reservedConcurrentExecutions:
        Token.asNumber(this.getParameterFromSsm("ehokshandler-concurrency")),
      timeout: Duration.seconds(
        Token.asNumber(this.getParameterFromSsm("ehokshandler-timeout"))
      ),
      tracing: lambda.Tracing.ACTIVE
    });
    AMISherateTable.grantReadWriteData(AMISHerateHandler);
    organisaatioWhitelistTable.grantReadData(AMISHerateHandler);

    AMISHerateHandler.addEventSource(new SqsEventSource(ehoksHerateQueue, { batchSize: 1, }));

    const AMISherateEmailHandler = new lambda.Function(this, "AMISHerateEmailHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        herate_table: AMISherateTable.tableName,
        caller_id: `${id}-AMISherateEmailHandler`,
      },
      memorySize: Token.asNumber(this.getParameterFromSsm("emailhandler-memory")),
      timeout: Duration.seconds(
        Token.asNumber(this.getParameterFromSsm("emailhandler-timeout"))
      ),
      handler: "oph.heratepalvelu.AMISherateEmailHandler::handleSendAMISEmails",
      tracing: lambda.Tracing.ACTIVE
    });
    AMISherateTable.grantReadWriteData(AMISherateEmailHandler);

    new events.Rule(this, "AMISHerateEmailScheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${this.getParameterFromSsm("emailhandler-cron")})`
      ),
      targets: [new targets.LambdaFunction(AMISherateEmailHandler)]
    });

    const updatedOoHandler = new lambda.Function(this, "UpdatedOOHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        herate_table: AMISherateTable.tableName,
        metadata_table: metadataTable.tableName,
        orgwhitelist_table: organisaatioWhitelistTable.tableName,
        caller_id: `${id}-updatedOpiskeluoikeusHandler`,
      },
      handler:
        "oph.heratepalvelu.UpdatedOpiskeluoikeusHandler::handleUpdatedOpiskeluoikeus",
      memorySize: Token.asNumber(
        this.getParameterFromSsm("updatedoohandler-memory")
      ),
      timeout: Duration.seconds(
        Token.asNumber(this.getParameterFromSsm("updatedoohandler-timeout"))
      ),
      tracing: lambda.Tracing.ACTIVE
    });
    metadataTable.grantReadWriteData(updatedOoHandler);
    AMISherateTable.grantReadWriteData(updatedOoHandler);
    organisaatioWhitelistTable.grantReadData(updatedOoHandler);

    new events.Rule(this, "UpdatedOoScheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${this.getParameterFromSsm("updatedoohandler-cron")}))`
      ),
      targets: [new targets.LambdaFunction(updatedOoHandler)]
    });

    // const migrateHandler = new lambda.Function(this, "migrateHandler", {
    //   runtime: lambda.Runtime.JAVA_8,
    //   code: lambdaCode,
    //   environment: {
    //     from_table: "",
    //     to_table: AMISherateTable.tableName
    //   },
    //   handler: "oph.heratepalvelu.migrateHandler::handleMigration",
    //   memorySize: 2048,
    //   timeout: Duration.seconds(
    //     15 * 60
    //   ),
    //   tracing: lambda.Tracing.ACTIVE
    // });

    const dlqResendHandler = new lambda.Function(this, "DLQresendHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        queue_name: ehoksHerateQueue.queueName
      },
      handler: "oph.heratepalvelu.DLQresendHandler::handleDLQresend",
      memorySize: 1024,
      timeout: Duration.seconds(30),
      tracing: lambda.Tracing.ACTIVE
    });

    dlqResendHandler.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [ehoksHerateQueue.queueArn, herateDeadLetterQueue.queueArn],
      actions: [
        "sqs:GetQueueUrl",
        "sqs:SendMessage",
        "sqs:ReceiveMessage",
        "sqs:ChangeMessageVisibility",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ]}));

    const eHOKSMaintenanceHandler = new lambda.Function(this, "eHOKSMaintenanceHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        metadata_table: metadataTable.tableName,
      },
      handler: "oph.heratepalvelu.eHOKSMaintenanceHandler::handleMaintenance",
      memorySize: 512,
      tracing: lambda.Tracing.ACTIVE
    });
    metadataTable.grantReadWriteData(eHOKSMaintenanceHandler);

    new CfnEventSourceMapping(this, "DLQResendEventSourceMapping", {
      eventSourceArn: herateDeadLetterQueue.queueArn,
      functionName: dlqResendHandler.functionName,
      batchSize: 1,
      enabled: false
    });

    [AMISHerateHandler, AMISherateEmailHandler, updatedOoHandler].forEach(
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
