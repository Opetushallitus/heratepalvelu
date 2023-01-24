#!/usr/bin/env node
import cdk = require("@aws-cdk/core");
import { HeratepalveluAMISStack } from "../lib/amis";
import { HeratepalveluTEPStack } from "../lib/tep";
import { HeratepalveluTPKStack } from "../lib/tpk";
import { HeratepalveluTEPRAHOITUSStack } from "../lib/teprah";

const git = require("git-utils");
const repo = git.open(".");

const status = repo.getStatus();
const aheadBehindCount = repo.getAheadBehindCount();
const upstreamBranch = repo.getUpstreamBranch();

if (!upstreamBranch) {
  throw new Error("No upstream branch");
}

const canDeploy = true || !(Object.entries(status).length !== 0 || aheadBehindCount.ahead !== 0 || aheadBehindCount.behind !== 0);

const version = canDeploy ? repo.getReferenceTarget(repo.getHead()) : "uncommitted";

const app = new cdk.App();

new HeratepalveluAMISStack(app, "sieni-services-heratepalvelu", 'sieni', version);
const tepSieni = new HeratepalveluTEPStack(app, "sieni-services-heratepalvelu-tep", 'sieni', version);
new HeratepalveluTEPRAHOITUSStack(app, "sieni-services-heratepalvelu-teprah", 'sieni', version, tepSieni.jaksotunnusTable);
new HeratepalveluTPKStack(app, "sieni-services-heratepalvelu-tpk", 'sieni', version, tepSieni.jaksotunnusTable);

if (canDeploy) {
  new HeratepalveluAMISStack(app, "pallero-services-heratepalvelu", 'pallero', version);
  const tepPallero = new HeratepalveluTEPStack(app, "pallero-services-heratepalvelu-tep", 'pallero', version);
  new HeratepalveluTEPRAHOITUSStack(app, "pallero-services-heratepalvelu-teprah", 'pallero', version, tepPallero.jaksotunnusTable);
  new HeratepalveluTPKStack(app, "pallero-services-heratepalvelu-tpk", 'pallero', version, tepPallero.jaksotunnusTable);

  if (upstreamBranch === "refs/remotes/origin/master") {
    new HeratepalveluAMISStack(app, "sade-services-heratepalvelu", 'sade', version);
    const tepSade = new HeratepalveluTEPStack(app, "sade-services-heratepalvelu-tep", 'sade', version);
    new HeratepalveluTEPRAHOITUSStack(app, "sade-services-heratepalvelu-teprah", 'sade', version, tepSade.jaksotunnusTable);
    new HeratepalveluTPKStack(app, "sade-services-heratepalvelu-tpk", 'sade', version, tepSade.jaksotunnusTable);
  } else {
    console.log("\nNOT IN MASTER BRANCH!!!\n")
  }
} else {
  console.log("\nUncommited changes or local is ahead/behind of remote:\n");
  console.log(status);
  console.log(aheadBehindCount);
  console.log("\n");
}
