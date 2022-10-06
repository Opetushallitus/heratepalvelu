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

export class HeratepalveluTEPRAHOITUSStack extends HeratepalveluStack {
    constructor(
        scope: cdk.App,
        id: string,
        envName: string,
        version: string,
        tepJaksotunnusTable: dynamodb.Table,
        props?: cdk.StackProps
    ) {
        super(scope, id, envName, version, props);

        //SQS

        const tepRahoitusDLQ = new sqs.Queue(this, "RahoitusLaskentaDLQ", {
            retentionPeriod: Duration.days(14),
            visibilityTimeout: (Duration.seconds(60))
        });

        const tepRahoitusQueue = new sqs.Queue(this, "RahoitusLaskentaQueue", {
            queueName: `${id}-RahoitusLaskentaQueue`,
            deadLetterQueue: {
                queue: tepRahoitusDLQ,
                maxReceiveCount: 5
            },
            visibilityTimeout: Duration.seconds(60),
            retentionPeriod: Duration.days(14)
        });


        //Lambda handler for SQS

        //Dynamo Table

        const resultsTable = new dynamodb.Table(this, "tepRahoitusResultsTable", {
            partitionKey: {
                name: "hankkimistapa_id",
                type: dynamodb.AttributeType.NUMBER
            },
            billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
            serverSideEncryption: true
        });

        this.tepRahoitusResultsTable = resultsTable;


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


        const rahoitusResultsHandler = new lambda.Function(this, "TEPRahoitusLaskentaHandler", {
            runtime: lambda.Runtime.JAVA_8_CORRETTO,
            code: lambdaCode,
            environment: {
                ...this.envVars,
                results_table: resultsTable.tableName,
                jaksotunnus_table: tepJaksotunnusTable.tableName, //this should only be read from by the handler...
                caller_id: `1.2.246.562.10.00000000001.${id}-rahoitusLaskentaHandler`,
            },
            handler: "oph.heratepalvelu.tep.rahoitusLaskentaHandler::handleRahoitusHerate",
            memorySize: 1024,
            reservedConcurrentExecutions: 1, //fixme, parametrit kuntoon
            timeout: Duration.seconds(10),
            //tracing: lambda.Tracing.ACTIVE
        });

        rahoitusResultsHandler.addEventSource(new SqsEventSource(tepRahoitusQueue, { batchSize: 1 }));
        resultsTable.grantReadWriteData(rahoitusResultsHandler);
        tepJaksotunnusTable.grantReadData(rahoitusResultsHandler);

        [
            rahoitusResultsHandler
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

    tepRahoitusResultsTable: dynamodb.Table;
}
