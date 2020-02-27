import cdk = require("@aws-cdk/core");
import dynamodb = require("@aws-cdk/aws-dynamodb");
import lambda = require("@aws-cdk/aws-lambda");
import s3assets = require("@aws-cdk/aws-s3-assets");
import sqs = require("@aws-cdk/aws-sqs");
import iam = require("@aws-cdk/aws-iam");
import { SqsEventSource } from "@aws-cdk/aws-lambda-event-sources";
import { Duration, Token } from "@aws-cdk/core";
import { HeratepalveluStack } from "./heratepalvelu";


export class HeratepalveluTEPStack extends HeratepalveluStack {
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

    const TPOherateTable = new dynamodb.Table(this, "TPOHerateTable", {
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

    /////////////////
    // SQS
    /////////////////

    TPOherateTable.addGlobalSecondaryIndex({
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

    const TPOherateDeadLetterQueue = new sqs.Queue(this, "TPOHerateDeadLetterQueue", {
      retentionPeriod: Duration.days(14)
    });

    const TPOHerateQueue = new sqs.Queue(this, "TPOHerateQueue", {
      queueName: `${id}-TPOHerateQueue`,
      deadLetterQueue: {
        queue: TPOherateDeadLetterQueue,
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
      "EhoksHerateTEPLambdaAsset",
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

    const TPOHerateHandler = new lambda.Function(this, "TPOHerateHandler", {
      runtime: lambda.Runtime.JAVA_8,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        herate_table: TPOherateTable.tableName,
        caller_id: `${id}-TPOherateHandler`,
      },
      handler: "oph.heratepalvelu.TPOherateHandler::handleTPOherate",
      memorySize: Token.asNumber(this.getParameterFromSsm("tpohandler-memory")),
      reservedConcurrentExecutions:
        Token.asNumber(this.getParameterFromSsm("tpohandler-concurrency")),
      timeout: Duration.seconds(
        Token.asNumber(this.getParameterFromSsm("tpohandler-timeout"))
      ),
      tracing: lambda.Tracing.ACTIVE
    });
    TPOherateTable.grantReadWriteData(TPOHerateHandler);

    TPOHerateHandler.addEventSource(new SqsEventSource(TPOHerateQueue, { batchSize: 1 }));

    [TPOHerateHandler].forEach(
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
