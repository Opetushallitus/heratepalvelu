import { App, Duration, Fn, Token, StackProps } from 'aws-cdk-lib';
import dynamodb = require("aws-cdk-lib/aws-dynamodb");
import lambda = require("aws-cdk-lib/aws-lambda");
import ec2 = require("aws-cdk-lib/aws-ec2");
import s3assets = require("aws-cdk-lib/aws-s3-assets");
import sqs = require("aws-cdk-lib/aws-sqs");
import iam = require("aws-cdk-lib/aws-iam");
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { HeratepalveluStack } from "./heratepalvelu";
import { LogGroup, RetentionDays } from "aws-cdk-lib/aws-logs";

export class HeratepalveluTEPRAHOITUSStack extends HeratepalveluStack {
    constructor(
        scope: App,
        id: string,
        envName: string,
        version: string,
        tepJaksotunnusTable: dynamodb.Table,
        props?: StackProps
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
            visibilityTimeout: Duration.seconds(90),
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
            encryption: dynamodb.TableEncryption.AWS_MANAGED
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

        // LogGroup

        const teprahLogGroup = new LogGroup(this, 'TepRahLogGroup', {
            logGroupName: `${envName}-heratepalvelu-teprah`,
            retention: RetentionDays.TWO_YEARS,
        });

	// VPC

	const vpc = ec2.Vpc.fromVpcAttributes(this, "VPC", {
	  vpcId: Fn.importValue(`${envName}-Vpc`),
	  availabilityZones: [
	    Fn.importValue(`${envName}-SubnetAvailabilityZones`),
	  ],
	  privateSubnetIds: [
	    Fn.importValue(`${envName}-PrivateSubnet1`),
	    Fn.importValue(`${envName}-PrivateSubnet2`),
	    Fn.importValue(`${envName}-PrivateSubnet3`),
	  ],
	});

        // Lambda

        const lambdaCode = lambda.Code.fromBucket(
            ehoksHerateTEPAsset.bucket,
            ehoksHerateTEPAsset.s3ObjectKey
        );

        const virkailija_url = this.getEnvVarFromSsm("virkailija_url");

        const rahoitusResultsHandler = new lambda.Function(this, "TEPRahoitusLaskentaHandler", {
            runtime: this.runtime,
            code: lambdaCode,
            environment: {
                ...this.envVars,
                ehoks_url: `${virkailija_url}/ehoks-virkailija-backend-freeze/api/v1/`, //note -freeze
                ehoks_cas_base: `/ehoks-virkailija-backend-freeze`,
                results_table: resultsTable.tableName,
                jaksotunnus_table: tepJaksotunnusTable.tableName, //this should only be read from by the handler...
                caller_id: `1.2.246.562.10.00000000001.${id}-rahoitusLaskentaHandler`,
            },
            handler: "oph.heratepalvelu.tep.rahoitusLaskentaHandler::handleRahoitusHerate",
            memorySize: 1024,
            reservedConcurrentExecutions: 2, //fixme, parametrit kuntoon
            timeout: Duration.seconds(80),
            tracing: lambda.Tracing.ACTIVE,
	    logGroup: teprahLogGroup,
	    vpc: vpc
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
