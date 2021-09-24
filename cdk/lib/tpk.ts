import dynamodb from "@aws-cdk/aws-dynamodb";
import events from "@aws-cdk/aws-events";
import targets from "@aws-cdk/aws-events-targets";
import iam from "@aws-cdk/aws-iam";
import lambda from "@aws-cdk/aws-lambda";
import s3assets from "@aws-cdk/aws-s3-assets";
import { Duration, Token } from "@aws-cdk/core";
import { HeratepalveluStack } from "./neratepalvelu";

export class HeratepalveluTPKStack extends HeratepalveluStack {
  constructor(
    scope: cdk.App,
    id: string,
    envName: string,
    version: string,
    tepJaksotunnusTable: Table,
    props?: cdk.StackProps,
  ) {
    super(scope, id, envName, version, props);

    // Dynamo DB

    const tpkNippuTable = new dynamodb.Table(this, "tpkNippuTable", {
      partitionKey: {
        name: "nippu-id",
        type: dynamodb.AttributeType.STRING
      },
      sortKey: {
        // TODO pick one — päivämääräkö?
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      serverSideEncryption: true,
    });

    // TODO add secondary indexes when/if we need them

    // S3

    const ehoksHerateTPKAsset = new s3assets.Asset(
      this,
      "EhoksHerateTPKLambdaAsset",
      {
        path: "../target/uberjar/heratepalvelu-0.1.0-SNAPSHOT-standalone.jar"
      }
    );

    // Lambda

    const lambdaCode = lambda.Code.fromBucket(
      ehoksHerateTPKAsset.bucket,
      ehoksHerateTPKAsset.s3ObjectKey
    );

    // tpkNiputusHandler

    const tpkNiputusHandler = new lambda.Function(this, "TPKNiputusHandler", {
      runtime: lambda.Runtime.JAVA_8_CORRETTO,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        jaksotunnus_table: tepJaksotunnusTable.tableName,
        tpk_nippu_table: tpkNippuTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-TPKNiputusHandler`,
      },
      memorySize: Token.asNumber(1024),
      timeout: Duration.seconds(900),
      handler: "oph.heratepalvelu.tpk.tpkNiputusHandler::handleTpkNiputus",
      tracing: lambda.Tracing.ACTIVE,
    });

    new events.Rule(this, "tpkNiputusHandlerScheduleRule", {
      schedule: events.Schedule.expression(`rate(20 minutes)`),
      targets: [new targets.LambdaFunction(tpkNiputusHandler)]
    });

    tepJaksotunnusTable.grantReadWriteData(tpkNiputusHandler);
    tpkNippuTable.grantReadWriteData(tpkNiputusHandler);

    [tpkNiputusHandler].forEach(
      lambdaFunction => lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        resources: [`arn:aws:ssm:eu-west-1:*:parameter/${envName}/services/heratepalvelu/*`],
        actions: ["ssm:GetParameter"],
      }))
    );
  }
}