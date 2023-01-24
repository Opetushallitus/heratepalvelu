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

    this.jaksotunnusTable = jaksotunnusTable;

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
      projectionType: dynamodb.ProjectionType.ALL
    });

    jaksotunnusTable.addGlobalSecondaryIndex({
      indexName: "tepDbChangerIndex",
      partitionKey: {
        name: "oppija_oid",
        type: dynamodb.AttributeType.STRING
      },
      sortKey: {
        name: "niputuspvm",
        type: dynamodb.AttributeType.STRING
      },
      projectionType: dynamodb.ProjectionType.ALL
    });

    jaksotunnusTable.addGlobalSecondaryIndex({
      indexName: "uniikkiusIndex",
      partitionKey: {
        name: "tunnus",
        type: dynamodb.AttributeType.STRING
      },
      nonKeyAttributes: [
        "request_id",
        "hoks_id"
      ],
      projectionType: dynamodb.ProjectionType.INCLUDE
    });

    jaksotunnusTable.addGlobalSecondaryIndex({
      indexName: "tpkNiputusIndex",
      partitionKey: {
        name: "tpk-niputuspvm",
        type: dynamodb.AttributeType.STRING
      },
      sortKey: {
        name: "jakso_loppupvm",
        type: dynamodb.AttributeType.STRING
      },
      nonKeyAttributes: [
        "hankkimistapa_id",
        "hankkimistapa_tyyppi",
        "koulutustoimija",
        "oppisopimuksen_perusta",
        "tyopaikan_nimi",
        "tyopaikan_ytunnus"
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

    nippuTable.addGlobalSecondaryIndex({
      indexName: "niputusIndex",
      partitionKey: {
        name: "kasittelytila",
        type: dynamodb.AttributeType.STRING
      },
      sortKey: {
        name: "niputuspvm",
        type: dynamodb.AttributeType.STRING
      },
      projectionType: dynamodb.ProjectionType.ALL
    });

    nippuTable.addGlobalSecondaryIndex({
      indexName: "smsIndex",
      partitionKey: {
        name: "sms_kasittelytila",
        type: dynamodb.AttributeType.STRING
      },
      sortKey: {
        name: "niputuspvm",
        type: dynamodb.AttributeType.STRING
      },
      projectionType: dynamodb.ProjectionType.ALL
    })

    nippuTable.addGlobalSecondaryIndex({
      indexName: "emailMuistutusIndex",
      partitionKey: {
        name: "muistutukset",
        type: dynamodb.AttributeType.NUMBER
      },
      sortKey: {
        name: "lahetyspvm",
        type: dynamodb.AttributeType.STRING
      },
      projectionType: dynamodb.ProjectionType.ALL
    });

    nippuTable.addGlobalSecondaryIndex({
      indexName: "smsMuistutusIndex",
      partitionKey: {
        name: "sms_muistutukset",
        type: dynamodb.AttributeType.NUMBER
      },
      sortKey: {
        name: "sms_lahetyspvm",
        type: dynamodb.AttributeType.STRING
      },
      projectionType: dynamodb.ProjectionType.ALL
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

    const jaksotunnusArchive2021_2022Table = new dynamodb.Table(
      this,
      "jaksotunnusArchive2021to2022Table",
      {
        partitionKey: {
          name: "hankkimistapa_id",
          type: dynamodb.AttributeType.NUMBER
        },
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
        serverSideEncryption: true
      }
    );

    const nippuArchive2021_2022Table = new dynamodb.Table(
      this,
      "nippuArchive2021to2022Table",
      {
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
        path: "../target/default+uberjar/heratepalvelu-0.1.0-SNAPSHOT-standalone.jar"
      }
    );

    // Lambda

    const lambdaCode = lambda.Code.fromBucket(
        ehoksHerateTEPAsset.bucket,
        ehoksHerateTEPAsset.s3ObjectKey
    );

    // herateHandler

    const timedOperationsHandler = new lambda.Function(this, "timedOperationsHandler", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        jaksotunnus_table: jaksotunnusTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-timedOperationsHandler`,
      },
      memorySize: Token.asNumber(1024),
      reservedConcurrentExecutions: 1,
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

    jaksotunnusTable.grantReadWriteData(timedOperationsHandler);

    // jaksoHandler

    const jaksoHandler = new lambda.Function(this, "TEPJaksoHandler", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
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
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        jaksotunnus_table: jaksotunnusTable.tableName,
        nippu_table: nippuTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-niputusHandler`,
      },
      memorySize: Token.asNumber(1024),
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(900),
      handler: "oph.heratepalvelu.tep.niputusHandler::handleNiputus",
      tracing: lambda.Tracing.ACTIVE
    });

    new events.Rule(this, "niputusHandlerScheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${this.getParameterFromSsm("tep-niputus-cron")})`
      ),
      targets: [new targets.LambdaFunction(niputusHandler)]
    });

    jaksotunnusTable.grantReadWriteData(niputusHandler);
    nippuTable.grantReadWriteData(niputusHandler);

    // emailHandler

    const emailHandler = new lambda.Function(this, "TEPemailHandler", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        jaksotunnus_table: jaksotunnusTable.tableName,
        nippu_table: nippuTable.tableName,
        orgwhitelist_table: organisaatioWhitelistTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-emailHandler`,
      },
      memorySize: Token.asNumber(1024),
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(300),
      handler: "oph.heratepalvelu.tep.emailHandler::handleSendTEPEmails",
      tracing: lambda.Tracing.ACTIVE
    });

    new events.Rule(this, "emailHandlerScheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${this.getParameterFromSsm("tep-email-cron")})`
      ),
      targets: [new targets.LambdaFunction(emailHandler)]
    });

    jaksotunnusTable.grantReadData(emailHandler);
    organisaatioWhitelistTable.grantReadData(emailHandler);
    nippuTable.grantReadWriteData(emailHandler);

    const emailStatusHandler = new lambda.Function(this, "TEPEmailStatusHandler", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        nippu_table: nippuTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-TEPEmailStatusHandler`
      },
      memorySize: Token.asNumber(1024),
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(300),
      handler: "oph.heratepalvelu.tep.StatusHandler::handleEmailStatus",
      tracing: lambda.Tracing.ACTIVE
    });

    new events.Rule(this, "TEPEmailStatusScheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${this.getParameterFromSsm("tep-email-cron")})`
      ),
      targets: [new targets.LambdaFunction(emailStatusHandler)]
    });

    nippuTable.grantReadWriteData(emailStatusHandler);

    const tepSmsHandler = new lambda.Function(this, "tepSmsHandler", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      handler: "oph.heratepalvelu.tep.tepSmsHandler::handleTepSmsSending",
      environment: {
        ...this.envVars,
        nippu_table: nippuTable.tableName,
        jaksotunnus_table: jaksotunnusTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-tepSmsHandler`,
        send_messages: (this.envVars.stage === 'sade').toString()
      },
      memorySize: Token.asNumber(this.getParameterFromSsm("smshandler-memory")),
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(
          Token.asNumber(this.getParameterFromSsm("smshandler-timeout"))
      ),
      tracing: lambda.Tracing.ACTIVE
    });

    new events.Rule(this, "SMSscheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${this.getParameterFromSsm("tep-email-cron")})`
      ),
      targets: [new targets.LambdaFunction(tepSmsHandler)]
    });

    nippuTable.grantReadWriteData(tepSmsHandler);
    jaksotunnusTable.grantReadData(tepSmsHandler);

    // tep Email muistutushandler

    const EmailMuistutusHandler = new lambda.Function(this, "EmailMuistutusHandler", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        nippu_table: nippuTable.tableName,
        jaksotunnus_table: jaksotunnusTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-EmailMuistutusHandler`
      },
      memorySize: Token.asNumber(this.getParameterFromSsm("emailhandler-memory")),
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(
          Token.asNumber(this.getParameterFromSsm("emailhandler-timeout"))
      ),
      handler: "oph.heratepalvelu.tep.EmailMuistutusHandler::handleSendEmailMuistutus",
      tracing: lambda.Tracing.ACTIVE
    });

    nippuTable.grantReadWriteData(EmailMuistutusHandler);
    jaksotunnusTable.grantReadData(EmailMuistutusHandler);

    new events.Rule(this, "tep-EmailMuistutusScheduleRule", {
      schedule: events.Schedule.expression(
          `cron(${this.getParameterFromSsm("tep-email-cron")})`
      ),
      targets: [new targets.LambdaFunction(EmailMuistutusHandler)]
    });


    // tep Sms muistutushandler

    const SmsMuistutusHandler = new lambda.Function(this, "SmsMuistutusHandler", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        nippu_table: nippuTable.tableName,
        jaksotunnus_table: jaksotunnusTable.tableName,
        send_messages: (this.envVars.stage === 'sade').toString(),
        caller_id: `1.2.246.562.10.00000000001.${id}-SmsMuistutusHandler`
      },
      handler: "oph.heratepalvelu.tep.SMSMuistutusHandler::handleSendSMSMuistutus",
      memorySize: Token.asNumber(this.getParameterFromSsm("emailhandler-memory")),
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(
          Token.asNumber(this.getParameterFromSsm("emailhandler-timeout"))
      ),
      tracing: lambda.Tracing.ACTIVE
    });

    nippuTable.grantReadWriteData(SmsMuistutusHandler);
    jaksotunnusTable.grantReadData(SmsMuistutusHandler);

    new events.Rule(this, "tep-SmsMuistutusScheduleRule", {
      schedule: events.Schedule.expression(
          `cron(${this.getParameterFromSsm("tep-email-cron")})`
      ),
      targets: [new targets.LambdaFunction(SmsMuistutusHandler)]
    });

    // DLQ tyhjennys

    const dlqResendHandler = new lambda.Function(this, "TEP-DLQresendHandler", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        queue_name: herateQueue.queueName
      },
      handler: "oph.heratepalvelu.util.DLQresendHandler::handleDLQresend",
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

    const dbChangerTep = new lambda.Function(this, "dbChangerTep", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        table: jaksotunnusTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-dbChangerTep`
      },
      handler: "oph.heratepalvelu.util.dbChanger::handleDBUpdateTep",
      memorySize: 1024,
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(900),
      tracing: lambda.Tracing.ACTIVE
    });

    jaksotunnusTable.grantReadWriteData(dbChangerTep);

    // Arkistointifunktiot
    /*
    const archiveJaksoTable = new lambda.Function(this, "archiveJaksoTable", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        jaksotunnus_table: jaksotunnusTable.tableName,
        archive_table_2021_2022: jaksotunnusArchive2021_2022Table.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-archiveJaksoTable`,
      },
      memorySize: Token.asNumber(1024),
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(900),
      handler: "oph.heratepalvelu.tep.archiveJaksoTable::archiveJaksoTable",
      tracing: lambda.Tracing.ACTIVE
    });

    jaksotunnusTable.grantReadWriteData(archiveJaksoTable);
    jaksotunnusArchive2021_2022Table.grantReadWriteData(archiveJaksoTable);

    const archiveNippuTable = new lambda.Function(this, "archiveNippuTable", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        nippu_table: nippuTable.tableName,
        archive_table_2021_2022: nippuArchive2021_2022Table.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-archiveJaksoTable`,
      },
      memorySize: Token.asNumber(1024),
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(900),
      handler: "oph.heratepalvelu.tep.archiveNippuTable::archiveNippuTable",
      tracing: lambda.Tracing.ACTIVE
    });

    nippuTable.grantReadWriteData(archiveNippuTable);
    nippuArchive2021_2022Table.grantReadWriteData(archiveNippuTable);
    */

    // IAM

    [
      jaksoHandler,
      timedOperationsHandler,
      niputusHandler,
      emailHandler,
      emailStatusHandler,
      tepSmsHandler,
      SmsMuistutusHandler,
      EmailMuistutusHandler,
      dbChangerTep,
      // archiveJaksoTable,
      // archiveNippuTable,
    ].forEach(
        lambdaFunction => {
          lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            resources: [`arn:aws:ssm:eu-west-1:*:parameter/${envName}/services/heratepalvelu/*`],
            actions: ['ssm:GetParameter']
          }));
        }
    );
  }

  jaksotunnusTable: dynamodb.Table;
}
