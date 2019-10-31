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
    props?: cdk.StackProps
  ) {
    super(scope, id, props);

    const git = require("git-utils");
    const repo = git.open(".");

    const status = repo.getStatus();
    const aheadBehindCount = repo.getAheadBehindCount();

    if (Object.entries(status).length !== 0
      || aheadBehindCount.ahead !== 0
      || aheadBehindCount.behind !== 0) {
      console.log("Uncommited changes or local is ahead/behind of remote:\n");
      console.log(status);
      console.log(aheadBehindCount);
      throw new Error();
    }

    Tag.add(this, "Deployed version", repo.getReferenceTarget(repo.getHead()));

    const getParameterFromSsm = (parameterName: string): string => {
      return ssm.StringParameter.valueForStringParameter(
        this,
        `/${envName}/serverless/heratepalvelu/${parameterName}`
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

    const ehoksHerateHandler = new lambda.Function(this, "EhoksHerateHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...envVars,
        caller_id: `${id}-eHOKSherateHandler`
      },
      handler: "oph.heratepalvelu.eHOKSherateHandler::handleHOKSherate",
      memorySize: Token.asNumber(getParameterFromSsm("ehokshandler-memory")),
      reservedConcurrentExecutions:
        Token.asNumber(getParameterFromSsm("ehokshandler-concurrency")),
      timeout: Duration.seconds(
        Token.asNumber(getParameterFromSsm("ehokshandler-timeout"))
      ),
      tracing: lambda.Tracing.ACTIVE
    });
    ehoksHerateAsset.grantRead(ehoksHerateHandler);

    ehoksHerateHandler.addEventSource(new SqsEventSource(ehoksHerateQueue, { batchSize: 1 }));

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
    ehoksHerateAsset.grantRead(herateEmailHandler);

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
        ehoks_url: `${envVars.virkailija_url}/ehoks-virkailija-backend/api/v1}`
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

    [ehoksHerateHandler, herateEmailHandler, updatedOoHandler].forEach(
      lambdaFunction => {
        metadataTable.grantReadWriteData(lambdaFunction);
        herateTable.grantReadWriteData(lambdaFunction);
        organisaatioWhitelistTable.grantReadData(lambdaFunction);
        lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          resources: [`arn:aws:ssm:eu-west-1:*:parameter/${envName}/serverless/heratepalvelu/*`],
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
