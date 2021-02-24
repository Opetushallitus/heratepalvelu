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
import { Duration, Tags, Token } from "@aws-cdk/core";
import { CfnEventSourceMapping } from "@aws-cdk/aws-lambda";
import {HeratepalveluStack} from "./heratepalvelu";


export class HeratepalveluTEPStack extends HeratepalveluStack {
  constructor(
    scope: cdk.App,
    id: string,
    envName: string,
    version: string,
    props?: cdk.StackProps
  ) {
    super(scope, id, envName, version, props);

    const ehoksHerateTEPAsset = new s3assets.Asset(
      this,
      "EhoksHerateTEPLambdaAsset",
      {
        path: "../target/uberjar/heratepalvelu-0.1.0-SNAPSHOT-standalone.jar"
      }
    );

    const lambdaCode = lambda.Code.fromBucket(
        ehoksHerateTEPAsset.bucket,
        ehoksHerateTEPAsset.s3ObjectKey
    );

  }
}
