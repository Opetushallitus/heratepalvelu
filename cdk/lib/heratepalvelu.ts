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
import {Duration, Tag, Token} from "@aws-cdk/core";


export class HeratepalveluStack extends cdk.Stack {
  constructor(
    scope: cdk.App,
    id: string,
    envName: string,
    version: string,
    props?: cdk.StackProps
  ) {
    super(scope, id, props);



    Tag.add(this, "Deployed version", version);

    const getParameterFromSsm = (parameterName: string): string => {
      return ssm.StringParameter.valueForStringParameter(
        this,
        `/${envName}/services/heratepalvelu/${parameterName}`
      );
    };

    const getEnvVarFromSsm = (parameterName: string): string => {
      return getParameterFromSsm(parameterName.replace("_", "-"));
    };

    const envVarsList = [
      "cas_user",
      "arvo_url",
      "arvo_user",
      "koski_url",
      "koski_user",
      "virkailija_url"
    ];

    const herateTable = new dynamodb.Table(this, "HerateTable", {
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

    herateTable.addGlobalSecondaryIndex({
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

    const AMISherateDeadLetterQueue = new sqs.Queue(this, "AMISHerateDeadLetterQueue", {
      retentionPeriod: Duration.days(14)
    });

    const AMISHerateQueue = new sqs.Queue(this, "AMISHerateQueue", {
      queueName: `${id}-eHOKSHerateQueue`,
      deadLetterQueue: {
        queue: AMISherateDeadLetterQueue,
        maxReceiveCount: 5
      },
      visibilityTimeout: Duration.seconds(60),
      retentionPeriod: Duration.days(14)
    });

    const TPOherateDeadLetterQueue = new sqs.Queue(this, "TPOHerateDeadLetterQueue", {
      retentionPeriod: Duration.days(14)
    });

    const TPOHerateQueue = new sqs.Queue(this, "TPOHerateQueue", {
      queueName: `${id}-eHOKSHerateQueue`,
      deadLetterQueue: {
        queue: TPOherateDeadLetterQueue,
        maxReceiveCount: 5
      },
      visibilityTimeout: Duration.seconds(60),
      retentionPeriod: Duration.days(14)
    });

    let envVars = envVarsList.reduce((envVarsObject: any, key: string) => {
      envVarsObject[key] = getEnvVarFromSsm(key);
      return envVarsObject;
    }, {});

    envVars = {
      ...envVars,
      herate_table: herateTable.tableName,
      orgwhitelist_table: organisaatioWhitelistTable.tableName,
      metadata_table: metadataTable.tableName,
      organisaatio_url: `${envVars.virkailija_url}/organisaatio-service/rest/organisaatio/v4/`,
      cas_url: `${envVars.virkailija_url}/cas`,
      stage: envName
    };

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
        ...envVars,
        caller_id: `${id}-AMISherateHandler`,
        ehoks_url: `${envVars.virkailija_url}/ehoks-virkailija-backend/api/v1/`
      },
      handler: "oph.heratepalvelu.AMISherateHandler::handleAMISherate",
      memorySize: Token.asNumber(getParameterFromSsm("ehokshandler-memory")),
      reservedConcurrentExecutions:
        Token.asNumber(getParameterFromSsm("ehokshandler-concurrency")),
      timeout: Duration.seconds(
        Token.asNumber(getParameterFromSsm("ehokshandler-timeout"))
      ),
      tracing: lambda.Tracing.ACTIVE
    });

    AMISHerateHandler.addEventSource(new SqsEventSource(AMISHerateQueue, { batchSize: 1 }));

    const herateEmailHandler = new lambda.Function(this, "HerateEmailHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...envVars,
        caller_id: `${id}-herateEmailHandler`,
        viestintapalvelu_url: `${envVars.virkailija_url}/ryhmasahkoposti-service/email`
      },
      memorySize: Token.asNumber(getParameterFromSsm("emailhandler-memory")),
      timeout: Duration.seconds(
        Token.asNumber(getParameterFromSsm("emailhandler-timeout"))
      ),
      handler: "oph.heratepalvelu.herateEmailHandler::handleSendEmails",
      tracing: lambda.Tracing.ACTIVE
    });

    new events.Rule(this, "HerateEmailScheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${getParameterFromSsm("emailhandler-cron")})`
      ),
      targets: [new targets.LambdaFunction(herateEmailHandler)]
    });

    const updatedOoHandler = new lambda.Function(this, "UpdatedOOHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...envVars,
        caller_id: `${id}-updatedOpiskeluoikeusHandler`,
        ehoks_url: `${envVars.virkailija_url}/ehoks-virkailija-backend/api/v1/`
      },
      handler:
        "oph.heratepalvelu.UpdatedOpiskeluoikeusHandler::handleUpdatedOpiskeluoikeus",
      memorySize: Token.asNumber(
        getParameterFromSsm("updatedoohandler-memory")
      ),
      timeout: Duration.seconds(
        Token.asNumber(getParameterFromSsm("updatedoohandler-timeout"))
      ),
      tracing: lambda.Tracing.ACTIVE
    });

    const TPOHerateHandler = new lambda.Function(this, "TPOHerateHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...envVars,
        caller_id: `${id}-TPOherateHandler`,
        ehoks_url: `${envVars.virkailija_url}/ehoks-virkailija-backend/api/v1/`
      },
      handler: "oph.heratepalvelu.TPOherateHandler::handleTPOherate",
      memorySize: Token.asNumber(getParameterFromSsm("tpohandler-memory")),
      reservedConcurrentExecutions:
        Token.asNumber(getParameterFromSsm("tpohandler-concurrency")),
      timeout: Duration.seconds(
        Token.asNumber(getParameterFromSsm("tpohandler-timeout"))
      ),
      tracing: lambda.Tracing.ACTIVE
    });

    TPOHerateHandler.addEventSource(new SqsEventSource(TPOHerateQueue, { batchSize: 1 }));


    [AMISHerateHandler, herateEmailHandler, updatedOoHandler, TPOHerateHandler].forEach(
      lambdaFunction => {
        metadataTable.grantReadWriteData(lambdaFunction);
        herateTable.grantReadWriteData(lambdaFunction);
        organisaatioWhitelistTable.grantReadData(lambdaFunction);
        lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          resources: [`arn:aws:ssm:eu-west-1:*:parameter/${envName}/services/heratepalvelu/*`],
          actions: ['ssm:GetParameter']
        }));
      }
    );

    new events.Rule(this, "UpdatedOoScheduleRule", {
      schedule: events.Schedule.expression(
        `cron(${getParameterFromSsm("updatedoohandler-cron")}))`
      ),
      targets: [new targets.LambdaFunction(updatedOoHandler)]
    });
  }
}
