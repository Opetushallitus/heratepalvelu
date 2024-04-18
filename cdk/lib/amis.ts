import { App, Duration, Token, StackProps } from 'aws-cdk-lib';
import dynamodb = require("aws-cdk-lib/aws-dynamodb");
import events = require("aws-cdk-lib/aws-events");
import targets = require("aws-cdk-lib/aws-events-targets");
import lambda = require("aws-cdk-lib/aws-lambda");
import s3assets = require("aws-cdk-lib/aws-s3-assets");
import sqs = require("aws-cdk-lib/aws-sqs");
import sns = require("aws-cdk-lib/aws-sns");
import snsSubs = require("aws-cdk-lib/aws-sns-subscriptions")
import iam = require("aws-cdk-lib/aws-iam");
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { CfnEventSourceMapping } from "aws-cdk-lib/aws-lambda";
import {HeratepalveluStack} from "./heratepalvelu";
import { RetentionDays } from "aws-cdk-lib/aws-logs";



export class HeratepalveluAMISStack extends HeratepalveluStack {
  constructor(
    scope: App,
    id: string,
    envName: string,
    version: string,
    props?: StackProps
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
      encryption: dynamodb.TableEncryption.AWS_MANAGED
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
      projectionType: dynamodb.ProjectionType.ALL
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

    AMISherateTable.addGlobalSecondaryIndex({
      indexName: "smsIndex",
      partitionKey: {
        name: "sms-lahetystila",
        type: dynamodb.AttributeType.STRING
      },
      sortKey: {
        name: "alkupvm",
        type: dynamodb.AttributeType.STRING
      },
      projectionType: dynamodb.ProjectionType.ALL
    });

    AMISherateTable.addGlobalSecondaryIndex({
      indexName: "ehoksIdIndex",
      partitionKey: {
        name: "ehoks-id",
        type: dynamodb.AttributeType.NUMBER
      },
      projectionType: dynamodb.ProjectionType.KEYS_ONLY
    })

    const organisaatioWhitelistTable = new dynamodb.Table(
      this,
      "OrganisaatioWhitelistTable",
      {
        partitionKey: {
          name: "organisaatio-oid",
          type: dynamodb.AttributeType.STRING
        },
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
        encryption: dynamodb.TableEncryption.AWS_MANAGED
      }
    );

    const metadataTable = new dynamodb.Table(this, "MetadataTable", {
      partitionKey: {
        name: "key",
        type: dynamodb.AttributeType.STRING
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      encryption: dynamodb.TableEncryption.AWS_MANAGED
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
      visibilityTimeout: Duration.minutes(5),
      retentionPeriod: Duration.days(14)
    });

    const ONRhenkilomodifyDLQ = new sqs.Queue(this, "ONRhenkilomodifyDLQ", {
      retentionPeriod: Duration.days(14),
      visibilityTimeout: (Duration.seconds(60))
    });

    const ONRhenkilomodifyQueue = new sqs.Queue(this, "ONRhenkilomodifyQueue", {
      queueName: `${id}-ehokshenkilomodify`,
      deadLetterQueue: {
        queue: ONRhenkilomodifyDLQ,
        maxReceiveCount: 5
      },
      visibilityTimeout: Duration.minutes(5),
      retentionPeriod: Duration.days(14)
    });

    const ONRhenkilomodifyTopic = sns.Topic.fromTopicArn(this, "ONRhenkilomodifyTopic",
        this.getParameterFromSsm("ONRhenkilomodifyARN"));

    ONRhenkilomodifyTopic.addSubscription(new snsSubs.SqsSubscription(ONRhenkilomodifyQueue));

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
      runtime: this.runtime,
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
      tracing: lambda.Tracing.ACTIVE,
      logRetention: RetentionDays.TWO_YEARS
    });

    AMISHerateHandler.addEventSource(new SqsEventSource(ehoksHerateQueue, { batchSize: 1, }));
    organisaatioWhitelistTable.grantReadData(AMISHerateHandler);

    const ONRhenkilomodifyHandler = new lambda.Function(this, "ONRhenkilomodifyHandler", {
      runtime: this.runtime,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        caller_id: `1.2.246.562.10.00000000001.${id}-ONRhenkilomodifyHandler`,
      },
      handler: "oph.heratepalvelu.util.ONRhenkilomodify::handleONRhenkilomodify",
      memorySize: Token.asNumber(this.getParameterFromSsm("ehokshandler-memory")),
      reservedConcurrentExecutions:
          Token.asNumber(this.getParameterFromSsm("ehokshandler-concurrency")),
      timeout: Duration.seconds(
          Token.asNumber(this.getParameterFromSsm("ehokshandler-timeout"))
      ),
      tracing: lambda.Tracing.ACTIVE,
      logRetention: RetentionDays.TWO_YEARS
    });

    new CfnEventSourceMapping(this, "ONRhenkilomodifyEventSourceMapping", {
      eventSourceArn: ONRhenkilomodifyQueue.queueArn,
      functionName: ONRhenkilomodifyHandler.functionName,
      batchSize: 1,
      maximumBatchingWindowInSeconds: 5,
    });

    ONRhenkilomodifyHandler.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [ONRhenkilomodifyQueue.queueArn, ONRhenkilomodifyDLQ.queueArn],
      actions: [
        "sqs:GetQueueUrl",
        "sqs:ReceiveMessage",
        "sqs:ChangeMessageVisibility",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ]}));

    ONRhenkilomodifyHandler.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [`arn:aws:ssm:eu-west-1:*:parameter/${envName}/services/heratepalvelu/*`],
      actions: ['ssm:GetParameter']
    }));

    const ONRdlqResendHandler = new lambda.Function(this, "ONR-DLQresendHandler", {
      runtime: this.runtime,
      code: lambdaCode,
      environment: {
        queue_name: ONRhenkilomodifyQueue.queueName
      },
      handler: "oph.heratepalvelu.util.ONRDLQresendHandler::handleONRDLQresend",
      memorySize: 1024,
      timeout: Duration.seconds(60),
      tracing: lambda.Tracing.ACTIVE,
      logRetention: RetentionDays.TWO_YEARS
    });

    ONRdlqResendHandler.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [ONRhenkilomodifyQueue.queueArn, ONRhenkilomodifyDLQ.queueArn],
      actions: [
        "sqs:GetQueueUrl",
        "sqs:SendMessage",
        "sqs:ReceiveMessage",
        "sqs:ChangeMessageVisibility",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ]}));

    new CfnEventSourceMapping(this, "ONR-DLQResendEventSourceMapping", {
      eventSourceArn: ONRhenkilomodifyDLQ.queueArn,
      functionName: ONRdlqResendHandler.functionName,
      batchSize: 1,
      maximumBatchingWindowInSeconds: 5,
      enabled: false
    });

    const AMISherateEmailHandler = new lambda.Function(this, "AMISHerateEmailHandler", {
      runtime: this.runtime,
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
      tracing: lambda.Tracing.ACTIVE,
      logRetention: RetentionDays.TWO_YEARS
    });

    new events.Rule(this, "AMISHerateEmailScheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${this.getParameterFromSsm("emailhandler-cron")})`
      ),
      targets: [new targets.LambdaFunction(AMISherateEmailHandler)]
    });

    const AMISEmailStatusHandler = new lambda.Function(this, "AMISEmailStatusHandler", {
      runtime: this.runtime,
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
      tracing: lambda.Tracing.ACTIVE,
      logRetention: RetentionDays.TWO_YEARS
    });

    new events.Rule(this, "AMISEmailStatusScheduleRule", {
      schedule: events.Schedule.expression(
          `cron(${this.getParameterFromSsm("emailhandler-cron")})`
      ),
      targets: [new targets.LambdaFunction(AMISEmailStatusHandler)]
    });

    const AMISMuistutusHandler = new lambda.Function(this, "AMISMuistutusHandler", {
      runtime: this.runtime,
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
      tracing: lambda.Tracing.ACTIVE,
      logRetention: RetentionDays.TWO_YEARS
    });

    new events.Rule(this, "AMISMuistutusScheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${this.getParameterFromSsm("emailhandler-cron")})`
      ),
      targets: [new targets.LambdaFunction(AMISMuistutusHandler)]
    });

    const AMISEmailResendHandler = new lambda.Function(this, "AMISEmailResendHandler", {
      runtime: this.runtime,
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
      tracing: lambda.Tracing.ACTIVE,
      logRetention: RetentionDays.TWO_YEARS
    });

    AMISEmailResendHandler.addEventSource(new SqsEventSource(ehoksAmisResendQueue, { batchSize: 1, }));

    const AMISSMSHandler = new lambda.Function(this, "AMISSMSHandler", {
      runtime: this.runtime,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        herate_table: AMISherateTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-AMISSMSHandler`,
        send_messages: (this.envVars.stage === 'sade').toString()
      },
      memorySize: Token.asNumber(this.getParameterFromSsm("emailhandler-memory")),
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(
        Token.asNumber(this.getParameterFromSsm("emailhandler-timeout"))
      ),
      handler: "oph.heratepalvelu.amis.AMISSMSHandler::handleAMISSMS",
      tracing: lambda.Tracing.ACTIVE,
      logRetention: RetentionDays.TWO_YEARS
    });

    new events.Rule(this, "AMISSMSScheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${this.getParameterFromSsm("emailhandler-cron")})`
      ),
      targets: [new targets.LambdaFunction(AMISSMSHandler)]
    });

    const updatedOoHandler = new lambda.Function(this, "UpdatedOOHandler", {
      runtime: this.runtime,
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
      tracing: lambda.Tracing.ACTIVE,
      logRetention: RetentionDays.TWO_YEARS
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
      runtime: this.runtime,
      code: lambdaCode,
      environment: {
        queue_name: ehoksHerateQueue.queueName
      },
      handler: "oph.heratepalvelu.util.DLQresendHandler::handleDLQresend",
      memorySize: 1024,
      timeout: Duration.seconds(60),
      tracing: lambda.Tracing.ACTIVE,
      logRetention: RetentionDays.TWO_YEARS
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
      runtime: this.runtime,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        herate_table: AMISherateTable.tableName,
        caller_id: `1.2.24.562.10.00000000001.${id}-AMISDeleteTunnusHandler`,
      },
      handler: "oph.heratepalvelu.amis.AMISDeleteTunnusHandler::handleDeleteTunnus",
      memorySize: 1024,
      timeout: Duration.seconds(60),
      tracing: lambda.Tracing.ACTIVE,
      logRetention: RetentionDays.TWO_YEARS
    });

    AMISDeleteTunnusHandler.addEventSource(
      new SqsEventSource(amisDeleteTunnusQueue, { batchSize: 1, })
    );

    const updateSmsLahetystila = new lambda.Function(this, "updateSmsLahetystila", {
      runtime: this.runtime,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        herate_table: AMISherateTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-updateSmsLahetystila`
      },
      handler: "oph.heratepalvelu.util.dbChanger::updateSmsLahetystila",
      memorySize: 1024,
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(900),
      tracing: lambda.Tracing.ACTIVE,
      logRetention: RetentionDays.TWO_YEARS
    });

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
        encryption: dynamodb.TableEncryption.AWS_MANAGED
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
        encryption: dynamodb.TableEncryption.AWS_MANAGED
      }
    );

    const AMISherateArchive2021_2022Table = new dynamodb.Table(
      this,
      "AMISHerateArchive2021to2022Table",
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
        encryption: dynamodb.TableEncryption.AWS_MANAGED
      }
    );

    const AMISherateArchive2022_2023Table = new dynamodb.Table(
      this,
      "AMISHerateArchive2022to2023Table",
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
        encryption: dynamodb.TableEncryption.AWS_MANAGED
      }
    );

    const AMISTimedOperationsHandler = new lambda.Function(
      this,
      "AMISTimedOperationsHandler",
      {
        runtime: this.runtime,
        code: lambdaCode,
        environment: {
          ...this.envVars,
          herate_table: AMISherateTable.tableName,
          caller_id: `1.2.246.562.10.00000000001.${id}-AMISTimedOperationsHandler`,
        },
        memorySize: Token.asNumber(1024),
        timeout: Duration.seconds(900),
        handler: "oph.heratepalvelu.amis.AMISehoksTimedOperationsHandler::handleAMISTimedOperations",
        tracing: lambda.Tracing.ACTIVE,
	logRetention: RetentionDays.TWO_YEARS
      }
    );

    new events.Rule(this, "AMISTimedOperationsScheduleRule", {
      schedule: events.Schedule.expression(
        `rate(${this.getParameterFromSsm("timedoperations-rate")})`
      ),
      targets: [new targets.LambdaFunction(AMISTimedOperationsHandler)]
    });

    const AMISMassHerateResendHandler = new lambda.Function(
      this,
      "AMISMassHerateResendHandler",
      {
        runtime: this.runtime,
        code: lambdaCode,
        environment: {
          ...this.envVars,
          caller_id: `1.2.246.562.10.00000000001.${id}-AMISMassHerateResendHandler`,
        },
        memorySize: Token.asNumber(1024),
        timeout: Duration.seconds(900),
        handler: "oph.heratepalvelu.amis.AMISehoksTimedOperationsHandler::handleMassHerateResend",
        tracing: lambda.Tracing.ACTIVE,
	logRetention: RetentionDays.TWO_YEARS
      }
    );

    new events.Rule(this, "AMISMassHerateResendScheduleRule", {
      schedule: events.Schedule.expression(
        `rate(7 days)`
      ),
      targets: [new targets.LambdaFunction(AMISMassHerateResendHandler)]
    });

    const EhoksOpiskeluoikeusUpdateHandler = new lambda.Function(
        this,
        "EhoksOpiskeluoikeusUpdateHandler",
        {
          runtime: this.runtime,
          code: lambdaCode,
          environment: {
            ...this.envVars,
            caller_id: `1.2.246.562.10.00000000001.${id}-EhoksOpiskeluoikeusUpdateHandler`,
          },
          memorySize: Token.asNumber(1024),
          timeout: Duration.seconds(900),
          handler: "oph.heratepalvelu.amis.AMISehoksTimedOperationsHandler::handleEhoksOpiskeluoikeusUpdate",
          tracing: lambda.Tracing.ACTIVE,
	  logRetention: RetentionDays.TWO_YEARS
        }
    );

    new events.Rule(this, "EhoksOpiskeluoikeusUpdateRule", {
      schedule: events.Schedule.expression(
          `rate(7 days)`
      ),
      targets: [new targets.LambdaFunction(EhoksOpiskeluoikeusUpdateHandler)]
    });

    const dbArchiver = new lambda.Function(this, "archiveHerateTable", {
      runtime: this.runtime,
      code:lambdaCode,
      environment: {
        ...this.envVars,
        from_table: AMISherateTable.tableName,
        to_table: AMISherateArchive2019_2020Table.tableName,
        to_table_2020_2021: AMISherateArchive2020_2021Table.tableName,
        to_table_2021_2022: AMISherateArchive2021_2022Table.tableName,
        to_table_2022_2023: AMISherateArchive2022_2023Table.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-AMISDBArchiver`,
      },
      handler: "oph.heratepalvelu.amis.archiveHerateTable::archiveHerateTable",
      memorySize: 1024,
      timeout: Duration.seconds(900),
      tracing: lambda.Tracing.ACTIVE,
    });

    AMISherateTable.grantReadWriteData(dbArchiver);
    AMISherateArchive2019_2020Table.grantReadWriteData(dbArchiver);
    AMISherateArchive2020_2021Table.grantReadWriteData(dbArchiver);
    AMISherateArchive2021_2022Table.grantReadWriteData(dbArchiver);
    AMISherateArchive2022_2023Table.grantReadWriteData(dbArchiver);

    [
      AMISHerateHandler,
      AMISherateEmailHandler,
      updatedOoHandler,
      AMISEmailResendHandler,
      AMISMuistutusHandler,
      AMISEmailStatusHandler,
      AMISSMSHandler,
      AMISDeleteTunnusHandler,
      AMISTimedOperationsHandler,
      AMISMassHerateResendHandler,
      EhoksOpiskeluoikeusUpdateHandler,
      updateSmsLahetystila,
      dbArchiver,
    ].forEach(
      lambdaFunction => {
        AMISherateTable.grantReadWriteData(lambdaFunction);
        lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          resources: [`arn:aws:ssm:eu-west-1:*:parameter/${envName}/services/heratepalvelu/*`],
          actions: ['ssm:GetParameter']
        }));
        this.createMetricFilters(lambdaFunction);
      }
    );
  }
}
