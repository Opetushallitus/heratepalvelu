import cdk = require("@aws-cdk/core");
import dynamodb = require("@aws-cdk/aws-dynamodb");
import events = require("@aws-cdk/aws-events");
import targets = require("@aws-cdk/aws-events-targets");
import lambda = require("@aws-cdk/aws-lambda");
import s3assets = require("@aws-cdk/aws-s3-assets");
import sqs = require("@aws-cdk/aws-sqs");
import ssm = require("@aws-cdk/aws-ssm");
import iam = require("@aws-cdk/aws-iam");
import { SqsEventSource } from "@aws-cdk/aws-lambda-event-sources";
import { Duration, Tags, Token } from "@aws-cdk/core";
import { CfnEventSourceMapping } from "@aws-cdk/aws-lambda";
import {HeratepalveluStack} from "./heratepalvelu";


export class HeratepalveluAMISStack extends HeratepalveluStack {
  constructor(
    scope: cdk.App,
    id: string,
    envName: string,
    version: string,
    props?: cdk.StackProps
  ) {
    super(scope, id, envName, version, props);

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

    AMISherateTable.addGlobalSecondaryIndex({
      indexName: "muistutusIndex",
      partitionKey: {
        name: "muistutukset",
        type: dynamodb.AttributeType.NUMBER
      },
      sortKey: {
        name: "lahetyspvm",
        type: dynamodb.AttributeType.STRING
      },
      nonKeyAttributes: [
        "sahkoposti",
        "kyselylinkki",
        "suorituskieli",
        "kyselytyyppi"
      ],
      projectionType: dynamodb.ProjectionType.INCLUDE
    });

    AMISherateTable.addGlobalSecondaryIndex({
      indexName: "resendIndex",
      partitionKey: {
        name: "kyselylinkki",
        type: dynamodb.AttributeType.STRING
      },
      nonKeyAttributes: [
        "sahkoposti",
        "kyselylinkki"
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

    const herateDeadLetterQueue = new sqs.Queue(this, "HerateDeadLetterQueue", {
      retentionPeriod: Duration.days(14),
      visibilityTimeout: (Duration.seconds(60))
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

    const ehoksAmisResendDLQueue = new sqs.Queue(this, "AmisResendDLQueue", {
      retentionPeriod: Duration.days(14)
    });

    const ehoksAmisResendQueue = new sqs.Queue(this, "AmisResendQueue", {
      queueName: `${id}-eHOKSAmisResendQueue`,
      deadLetterQueue: {
        queue: ehoksAmisResendDLQueue,
        maxReceiveCount: 5
      },
      visibilityTimeout: Duration.seconds(60),
      retentionPeriod: Duration.days(14)
    });

    const ehoksHerateAsset = new s3assets.Asset(
      this,
      "EhoksHerateLambdaAsset",
      {
        path: "../target/uberjar/heratepalvelu-0.1.0-SNAPSHOT-standalone.jar"
      }
    );

    const lambdaCode = lambda.Code.fromBucket(
      ehoksHerateAsset.bucket,
      ehoksHerateAsset.s3ObjectKey
    );

    const AMISHerateHandler = new lambda.Function(this, "AMISHerateHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        orgwhitelist_table: organisaatioWhitelistTable.tableName,
        herate_table: AMISherateTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-AMISherateHandler`,
        ehoks_url: `${this.envVars.virkailija_url}/ehoks-virkailija-backend/api/v1/`
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

    AMISHerateHandler.addEventSource(new SqsEventSource(ehoksHerateQueue, { batchSize: 1, }));
    organisaatioWhitelistTable.grantReadData(AMISHerateHandler);

    const AMISherateEmailHandler = new lambda.Function(this, "AMISHerateEmailHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        herate_table: AMISherateTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-AMISherateEmailHandler`,
        viestintapalvelu_url: `${this.envVars.virkailija_url}/ryhmasahkoposti-service/email`,
        ehoks_url: `${this.envVars.virkailija_url}/ehoks-virkailija-backend/api/v1/`
      },
      memorySize: Token.asNumber(this.getParameterFromSsm("emailhandler-memory")),
      timeout: Duration.seconds(
        Token.asNumber(this.getParameterFromSsm("emailhandler-timeout"))
      ),
      handler: "oph.heratepalvelu.AMISherateEmailHandler::handleSendAMISEmails",
      tracing: lambda.Tracing.ACTIVE
    });

    new events.Rule(this, "AMISHerateEmailScheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${this.getParameterFromSsm("emailhandler-cron")})`
      ),
      targets: [new targets.LambdaFunction(AMISherateEmailHandler)]
    });

    const AMISMuistutusHandler = new lambda.Function(this, "AMISMuistutusHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        herate_table: AMISherateTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-AMISMuistutusHandler`,
        viestintapalvelu_url: `${this.envVars.virkailija_url}/ryhmasahkoposti-service/email`,
        ehoks_url: `${this.envVars.virkailija_url}/ehoks-virkailija-backend/api/v1/`
      },
      memorySize: Token.asNumber(this.getParameterFromSsm("emailhandler-memory")),
      timeout: Duration.seconds(
        Token.asNumber(this.getParameterFromSsm("emailhandler-timeout"))
      ),
      handler: "oph.heratepalvelu.AMISMuistutusHandler::handleSendAMISMuistutus",
      tracing: lambda.Tracing.ACTIVE
    });

    new events.Rule(this, "AMISMuistutusScheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${this.getParameterFromSsm("emailhandler-cron")})`
      ),
      targets: [new targets.LambdaFunction(AMISMuistutusHandler)]
    });

    const AMISEmailResendHandler = new lambda.Function(this, "AMISEmailResendHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        herate_table: AMISherateTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-AMISEmailResendHandler`,
        viestintapalvelu_url: `${this.envVars.virkailija_url}/ryhmasahkoposti-service/email`,
        ehoks_url: `${this.envVars.virkailija_url}/ehoks-virkailija-backend/api/v1/`
      },
      handler: "oph.heratepalvelu.AMISEmailResendHandler::handleEmailResend",
      memorySize: 1024,
      timeout: Duration.seconds(60),
      tracing: lambda.Tracing.ACTIVE
    });

    AMISEmailResendHandler.addEventSource(new SqsEventSource(ehoksAmisResendQueue, { batchSize: 1, }));

    const updatedOoHandler = new lambda.Function(this, "UpdatedOOHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        orgwhitelist_table: organisaatioWhitelistTable.tableName,
        metadata_table: metadataTable.tableName,
        herate_table: AMISherateTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-updatedOpiskeluoikeusHandler`,
        ehoks_url: `${this.envVars.virkailija_url}/ehoks-virkailija-backend/api/v1/`
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

    new events.Rule(this, "UpdatedOoScheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${this.getParameterFromSsm("updatedoohandler-cron")}))`
      ),
      targets: [new targets.LambdaFunction(updatedOoHandler)]
    });

    organisaatioWhitelistTable.grantReadData(updatedOoHandler);
    metadataTable.grantReadWriteData(updatedOoHandler);

    const dlqResendHandler = new lambda.Function(this, "DLQresendHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        queue_name: ehoksHerateQueue.queueName
      },
      handler: "oph.heratepalvelu.DLQresendHandler::handleDLQresend",
      memorySize: 1024,
      timeout: Duration.seconds(60),
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

    new CfnEventSourceMapping(this, "DLQResendEventSourceMapping", {
      eventSourceArn: herateDeadLetterQueue.queueArn,
      functionName: dlqResendHandler.functionName,
      batchSize: 1,
      enabled: false
    });

    [AMISHerateHandler, AMISherateEmailHandler, updatedOoHandler,
      AMISEmailResendHandler, AMISMuistutusHandler
    ].forEach(
      lambdaFunction => {
        AMISherateTable.grantReadWriteData(lambdaFunction);
        lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          resources: [`arn:aws:ssm:eu-west-1:*:parameter/${envName}/services/heratepalvelu/*`],
          actions: ['ssm:GetParameter']
        }));
      }
    );
  }
}
