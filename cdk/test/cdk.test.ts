import { expect as expectCDK, haveResource } from '@aws-cdk/assert';
import { App } from '@aws-cdk/core';
import { HeratepalveluStack} from "../lib/heratepalvelu";

test('SQS Queue Created', () => {
  const app = new App();
  // WHEN
  const stack = new HeratepalveluStack(app, 'TestHeratePalveluStack', 'sieni', '1');
  // THEN
  expectCDK(stack).to(haveResource("AWS::SQS::Queue",{
     VisibilityTimeout: 300
  }));
});

test('SNS Topic Created', () => {
  const app = new App();
  // WHEN
  const stack = new HeratepalveluStack(app, 'TestHeratePalveluStack', 'sieni', '1');
  // THEN
  expectCDK(stack).to(haveResource("AWS::SNS::Topic"));
});