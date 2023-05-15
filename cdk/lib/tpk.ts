import { App, Duration, Token, StackProps } from 'aws-cdk-lib';
import dynamodb = require("aws-cdk-lib/aws-dynamodb");
import lambda = require("aws-cdk-lib/aws-lambda");
import s3assets = require("aws-cdk-lib/aws-s3-assets");
import iam = require("aws-cdk-lib/aws-iam");
import { HeratepalveluStack } from "./heratepalvelu";

export class HeratepalveluTPKStack extends HeratepalveluStack {
  constructor(
    scope: App,
    id: string,
    envName: string,
    version: string,
    tepJaksotunnusTable: dynamodb.Table,
    props?: StackProps,
  ) {
    super(scope, id, envName, version, props);

    // Dynamo DB

    const tpkNippuTable = new dynamodb.Table(this, "tpkNippuTable", {
      partitionKey: {
        name: "nippu-id",
        type: dynamodb.AttributeType.STRING
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      encryption: dynamodb.TableEncryption.AWS_MANAGED
    });

    const tpkNippuArchive2021FallTable = new dynamodb.Table(
      this,
      "tpkNippuArchive2021FallTable",
      {
        partitionKey: {
          name: "nippu-id",
          type: dynamodb.AttributeType.STRING
        },
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
        encryption: dynamodb.TableEncryption.AWS_MANAGED
      }
    );

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

    // Handlers

    const tpkNiputusHandler = new lambda.Function(this, "TPKNiputusHandler", {
      runtime: this.runtime,
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

    tepJaksotunnusTable.grantReadWriteData(tpkNiputusHandler);
    tpkNippuTable.grantReadWriteData(tpkNiputusHandler);

    const tpkArvoCallHandler = new lambda.Function(this, "TPKArvoCallHandler", {
      runtime: this.runtime,
      code: lambdaCode,
      environment: {
        ...this.envVars,
        tpk_nippu_table: tpkNippuTable.tableName,
        caller_id: `1.2.246.562.10.00000000001.${id}-TPKArvoCallHandler`,
      },
      memorySize: Token.asNumber(1024),
      timeout: Duration.seconds(900),
      handler: "oph.heratepalvelu.tpk.tpkArvoCallHandler::handleTpkArvoCalls",
      tracing: lambda.Tracing.ACTIVE,
    });

    tpkNippuTable.grantReadWriteData(tpkArvoCallHandler);

 /*   const archiveTpkNippuTable = new lambda.Function(
      this,
      "archiveTpkNippuTable",
      {
        runtime: this.runtime,
        code: lambdaCode,
        environment: {
          ...this.envVars,
          tpk_nippu_table: tpkNippuTable.tableName,
          archive_table_2021_fall: tpkNippuArchive2021FallTable.tableName,
          caller_id: `1.2.246.562.10.00000000001.${id}-archiveTpkNippuTable`,
        },
        memorySize: Token.asNumber(1024),
        timeout: Duration.seconds(900),
        handler: "oph.heratepalvelu.tpk.archiveTpkNippuTable::archiveTpkNippuTable",
        tracing: lambda.Tracing.ACTIVE,
      }
    );

    tpkNippuTable.grantReadWriteData(archiveTpkNippuTable);
    tpkNippuArchive2021FallTable.grantReadWriteData(archiveTpkNippuTable);*/

    [
      tpkNiputusHandler,
      tpkArvoCallHandler,
      //archiveTpkNippuTable,
    ].forEach(
      lambdaFunction => lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        resources: [`arn:aws:ssm:eu-west-1:*:parameter/${envName}/services/heratepalvelu/*`],
        actions: ["ssm:GetParameter"],
      }))
    );
  }
}
