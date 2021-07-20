#!/usr/bin/env node
import cdk = require("@aws-cdk/core");
import { HeratepalveluAMISStack } from "../lib/amis";
import { HeratepalveluTEPStack } from "../lib/tep";

const git = require("git-utils");
const repo = git.open(".");

const status = repo.getStatus();
const aheadBehindCount = repo.getAheadBehindCount();
const upstreamBranch = repo.getUpstreamBranch();

if (!upstreamBranch) {
  throw new Error("No upstream branch");
}

const canDeploy = !(Object.entries(status).length !== 0 || aheadBehindCount.ahead !== 0 || aheadBehindCount.behind !== 0);

const version = canDeploy ? repo.getReferenceTarget(repo.getHead()) : "uncommitted";

const app = new cdk.App();

new HeratepalveluAMISStack(app, "sieni-services-heratepalvelu", 'sieni', version);
new HeratepalveluTEPStack(app, "sieni-services-heratepalvelu-tep", 'sieni', version);

if (canDeploy) {
  new HeratepalveluAMISStack(app, "pallero-services-heratepalvelu", 'pallero', version);
  new HeratepalveluTEPStack(app, "pallero-services-heratepalvelu-tep", 'pallero', version);

  if (upstreamBranch === "refs/remotes/origin/master") {
    new HeratepalveluAMISStack(app, "sade-services-heratepalvelu", 'sade', version);
    new HeratepalveluTEPStack(app, "sade-services-heratepalvelu-tep", 'sade', version);
  } else {
    console.log("\nNOT IN MASTER BRANCH!!!\n")
  }
} else {
  console.log("\nUncommited changes or local is ahead/behind of remote:\n");
  console.log(status);
  console.log(aheadBehindCount);
  console.log("\n");
}
