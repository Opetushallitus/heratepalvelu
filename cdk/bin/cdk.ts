#!/usr/bin/env node
import cdk = require("@aws-cdk/core");
import { HeratepalveluStack } from "../lib/heratepalvelu";

const git = require("git-utils");
const repo = git.open(".");

const status = repo.getStatus();
const aheadBehindCount = repo.getAheadBehindCount();
const upstreamBranch = repo.getUpstreamBranch();

if (!upstreamBranch) {
  throw new Error("No upstream branch");
}

const canDeploy = !(Object.entries(status).length !== 0 || aheadBehindCount.ahead !== 0 || aheadBehindCount.behind !== 0);

const version = repo.getReferenceTarget(repo.getHead());

const app = new cdk.App();

if (canDeploy) {
  new HeratepalveluStack(app, "sieni-services-heratepalvelu", 'sieni', version);
  new HeratepalveluStack(app, "pallero-services-heratepalvelu", 'pallero', version);

  if (upstreamBranch === "refs/remotes/origin/master") {
    new HeratepalveluStack(app, "sade-services-heratepalvelu", 'sade', version);
  }
} else {
  console.log("\nUncommited changes or local is ahead/behind of remote:\n");
  console.log(status);
  console.log(aheadBehindCount);
  console.log("\n");

  new HeratepalveluStack(app, "sieni-services-heratepalvelu", 'sieni', version);
}