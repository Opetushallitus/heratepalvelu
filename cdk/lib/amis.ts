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

    const deleteTunnusDLQueue = new sqs.Queue(this, "AmisDeleteTunnusDLQueue", {
      retentionPeriod: Duration.days(14)
    });

    const amisDeleteTunnusQueue = new sqs.Queue(this, "AmisDeleteTunnusQueue", {
      queueName: `${id}-amisDeleteTunnusQueue`,
      deadLetterQueue: {
        queue: deleteTunnusDLQueue,
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
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        orgwhitelist_table: organisaatioWhitelistTable.tableName,
        herate_table: AMISherateTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-AMISherateHandler`,
      },
      handler: "oph.heratepalvelu.amis.AMISherateHandler::handleAMISherate",
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
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        herate_table: AMISherateTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-AMISherateEmailHandler`,
      },
      memorySize: Token.asNumber(this.getParameterFromSsm("emailhandler-memory")),
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(
        Token.asNumber(this.getParameterFromSsm("emailhandler-timeout"))
      ),
      handler: "oph.heratepalvelu.amis.AMISherateEmailHandler::handleSendAMISEmails",
      tracing: lambda.Tracing.ACTIVE
    });

    new events.Rule(this, "AMISHerateEmailScheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${this.getParameterFromSsm("emailhandler-cron")})`
      ),
      targets: [new targets.LambdaFunction(AMISherateEmailHandler)]
    });

    const AMISEmailStatusHandler = new lambda.Function(this, "AMISEmailStatusHandler", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        herate_table: AMISherateTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-AMISEmailStatusHandler`
      },
      memorySize: Token.asNumber(this.getParameterFromSsm("emailhandler-memory")),
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(
          Token.asNumber(this.getParameterFromSsm("emailhandler-timeout"))
      ),
      handler: "oph.heratepalvelu.amis.EmailStatusHandler::handleEmailStatus",
      tracing: lambda.Tracing.ACTIVE
    });

    new events.Rule(this, "AMISEmailStatusScheduleRule", {
      schedule: events.Schedule.expression(
          `cron(${this.getParameterFromSsm("emailhandler-cron")})`
      ),
      targets: [new targets.LambdaFunction(AMISEmailStatusHandler)]
    });

    const AMISMuistutusHandler = new lambda.Function(this, "AMISMuistutusHandler", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        herate_table: AMISherateTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-AMISMuistutusHandler`,
      },
      memorySize: Token.asNumber(this.getParameterFromSsm("emailhandler-memory")),
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(
        Token.asNumber(this.getParameterFromSsm("emailhandler-timeout"))
      ),
      handler: "oph.heratepalvelu.amis.AMISMuistutusHandler::handleSendAMISMuistutus",
      tracing: lambda.Tracing.ACTIVE
    });

    new events.Rule(this, "AMISMuistutusScheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${this.getParameterFromSsm("emailhandler-cron")})`
      ),
      targets: [new targets.LambdaFunction(AMISMuistutusHandler)]
    });

    const AMISEmailResendHandler = new lambda.Function(this, "AMISEmailResendHandler", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        herate_table: AMISherateTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-AMISEmailResendHandler`,
      },
      handler: "oph.heratepalvelu.amis.AMISEmailResendHandler::handleEmailResend",
      memorySize: 1024,
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(60),
      tracing: lambda.Tracing.ACTIVE
    });

    AMISEmailResendHandler.addEventSource(new SqsEventSource(ehoksAmisResendQueue, { batchSize: 1, }));

    const updatedOoHandler = new lambda.Function(this, "UpdatedOOHandler", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        orgwhitelist_table: organisaatioWhitelistTable.tableName,
        metadata_table: metadataTable.tableName,
        herate_table: AMISherateTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-updatedOpiskeluoikeusHandler`,
      },
      handler:
        "oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler::handleUpdatedOpiskeluoikeus",
      memorySize: Token.asNumber(
          this.getParameterFromSsm("updatedoohandler-memory")
      ),
      reservedConcurrentExecutions: 1,
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

    const dlqResendHandler = new lambda.Function(this, "AMIS-DLQresendHandler", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        queue_name: ehoksHerateQueue.queueName
      },
      handler: "oph.heratepalvelu.util.DLQresendHandler::handleDLQresend",
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

    const AMISDeleteTunnusHandler = new lambda.Function(this, "AMISDeleteTunnusHandler", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        herate_table: AMISherateTable.tableName,
        caller_id: `1.2.24.562.10.00000000001.${id}-AMISDeleteTunnusHandler`,
      },
      handler: "oph.heratepalvelu.amis.AMISDeleteTunnusHandler::handleDeleteTunnus",
      memorySize: 1024,
      timeout: Duration.seconds(60),
      tracing: lambda.Tracing.ACTIVE
    });

    AMISDeleteTunnusHandler.addEventSource(
      new SqsEventSource(amisDeleteTunnusQueue, { batchSize: 1, })
    );

    const AMISherateArchive2019_2020Table = new dynamodb.Table(
      this,
      "AMISHerateArchive2019to2020Table",
      {
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
      }
    );

    const AMISherateArchive2020_2021Table = new dynamodb.Table(
      this,
      "AMISHerateArchive2020to2021Table",
      {
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
      }
    );

    /*const dbArchiver = new lambda.Function(this, "AMIS-DBArchiver", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code:lambdaCode,
      environment: {
        ...this.envVars,
        from_table: AMISherateTable.tableName,
        to_table: AMISherateArchive2019_2020Table.tableName,
        to_table_2020_2021: AMISherateArchive2020_2021Table.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-AMISDBArchiver`,
      },
      handler: "oph.heratepalvelu.util.dbArchiver::handleDBArchiving",
      memorySize: 1024,
      timeout: Duration.seconds(900),
      tracing: lambda.Tracing.ACTIVE,
    });

    AMISherateTable.grantReadWriteData(dbArchiver);
    AMISherateArchive2019_2020Table.grantReadWriteData(dbArchiver);
    AMISherateArchive2020_2021Table.grantReadWriteData(dbArchiver);*/

   /* const dbChanger = new lambda.Function(this, "AMIS-DBChanger", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        table: AMISherateTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-AMISherateHandler`,
      },
      handler: "oph.heratepalvelu.util.dbChanger::handleDBMarkIncorrectSuoritustyypit",
      memorySize: 1024,
      timeout: Duration.seconds(900),
      tracing: lambda.Tracing.ACTIVE
    });*/

    const eh1269dbChanger = new lambda.Function(this, "EH1269dbChanger", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        table: AMISherateTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-EH1269dbChanger`
      },
      handler: "oph.heratepalvelu.util.dbChanger::handleEH1269",
      memorySize: 1024,
      timeout: Duration.seconds(900),
      tracing: lambda.Tracing.ACTIVE
    });

    AMISherateTable.grantReadWriteData(eh1269dbChanger);

    [
      AMISHerateHandler,
      AMISherateEmailHandler,
      updatedOoHandler,
      AMISEmailResendHandler,
      AMISMuistutusHandler,
      AMISEmailStatusHandler,
      AMISDeleteTunnusHandler,
      eh1269dbChanger,
      //dbArchiver,
      // dbChanger
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
