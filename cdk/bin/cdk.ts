#!/usr/bin/env node
import cdk = require("@aws-cdk/core");
import { HeratepalveluStack } from "../lib/heratepalvelu";

const app = new cdk.App();
new HeratepalveluStack(app, "sieni-services-heratepalvelu", 'sieni');
new HeratepalveluStack(app, "pallero-services-heratepalvelu", 'pallero');
new HeratepalveluStack(app, "sade-services-heratepalvelu", 'sade');