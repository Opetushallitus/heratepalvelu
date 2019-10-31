#!/usr/bin/env node
import cdk = require("@aws-cdk/core");
import { HeratepalveluStack } from "../lib/heratepalvelu";

const app = new cdk.App();
new HeratepalveluStack(app, "sieni-heratepalvelu", 'sieni');
new HeratepalveluStack(app, "pallero-heratepalvelu", 'pallero');
new HeratepalveluStack(app, "sade-heratepalvelu", 'sade');