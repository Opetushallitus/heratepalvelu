#!/usr/bin/env node
import { App } from "aws-cdk-lib";
import { HeratepalveluAMISStack } from "../lib/amis";
import { HeratepalveluTEPStack } from "../lib/tep";
import { HeratepalveluTPKStack } from "../lib/tpk";
import { HeratepalveluTEPRAHOITUSStack } from "../lib/teprah";
import { simpleGit, SimpleGitOptions } from "simple-git";

const checkGitStatus = async () => {
  const options: Partial<SimpleGitOptions> = {
    baseDir: process.cwd(),
    binary: 'git',
    maxConcurrentProcesses: 6,
    trimmed: false,
  };
  const git = simpleGit(options);

  const status = await git.status();
  const gitLog = await git.log();
  const upstreamBranch = await git.getRemotes();

  if (upstreamBranch.length === 0) {
    throw new Error("No upstream branch");
  }

  const version = status.isClean() ? gitLog.latest?.hash ?? "uncommitted" : "uncommitted";

  const app = new App();

  new HeratepalveluAMISStack(app, "sieni-services-heratepalvelu", 'sieni', version);
  const tepSieni = new HeratepalveluTEPStack(app, "sieni-services-heratepalvelu-tep", 'sieni', version);
  new HeratepalveluTEPRAHOITUSStack(app, "sieni-services-heratepalvelu-teprah", 'sieni', version, tepSieni.jaksotunnusTable);
  new HeratepalveluTPKStack(app, "sieni-services-heratepalvelu-tpk", 'sieni', version, tepSieni.jaksotunnusTable);

  if (status.isClean() && status.ahead === 0 && status.behind === 0) {
    new HeratepalveluAMISStack(app, "pallero-services-heratepalvelu", 'pallero', version);
    const tepPallero = new HeratepalveluTEPStack(app, "pallero-services-heratepalvelu-tep", 'pallero', version);
    new HeratepalveluTEPRAHOITUSStack(app, "pallero-services-heratepalvelu-teprah", 'pallero', version, tepPallero.jaksotunnusTable);
    new HeratepalveluTPKStack(app, "pallero-services-heratepalvelu-tpk", 'pallero', version, tepPallero.jaksotunnusTable);

    if (status.tracking === "origin/master") {
      new HeratepalveluAMISStack(app, "sade-services-heratepalvelu", 'sade', version);
      const tepSade = new HeratepalveluTEPStack(app, "sade-services-heratepalvelu-tep", 'sade', version);
      new HeratepalveluTEPRAHOITUSStack(app, "sade-services-heratepalvelu-teprah", 'sade', version, tepSade.jaksotunnusTable);
      new HeratepalveluTPKStack(app, "sade-services-heratepalvelu-tpk", 'sade', version, tepSade.jaksotunnusTable);
    } else {
      console.log("\nNOT IN MASTER BRANCH!!!\n")
    }
  } else {
    console.log("\nUncommited changes or local is ahead/behind of remote, so not doing anything:\n");
    console.log(status);
    console.log("\n");
  }
};

checkGitStatus()
  .then(() => {
    console.log("Done checking git status.");
  })
  .catch((e) => {
    console.log("Error in git status check:", e);
    process.exit(1);
  });
