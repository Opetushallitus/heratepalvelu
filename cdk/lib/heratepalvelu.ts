import { Stack, App, StackProps, Tags } from 'aws-cdk-lib';
import lambda = require("aws-cdk-lib/aws-lambda");
import ssm = require("aws-cdk-lib/aws-ssm");

export type EnvVars = {
  virkailija_url: string
  organisaatio_url: string
  cas_url: string
  cas_user: string
  koski_url: string
  koski_user: string
  arvo_url: string
  arvo_user: string
  stage: string
}

export class HeratepalveluStack extends Stack {
  envName: string;
  envVars: EnvVars;
  runtime: lambda.Runtime

  constructor(
    scope: App,
    id: string,
    envName: string,
    version: string,
    props?: StackProps
  ) {
    super(scope, id, props);

    this.envName = envName;

    Tags.of(this).add("Deployed version", version);

    const envVarsList = [
      "cas_user",
      "arvo_url",
      "arvo_user",
      "koski_url",
      "koski_user",
      "virkailija_url"
    ];

    const envVarsTemp = envVarsList.reduce((envVarsObject: any, key: string) => {
      envVarsObject[key] = this.getEnvVarFromSsm(key);
      return envVarsObject;
    }, {});

    this.envVars = {
      ...envVarsTemp,
      organisaatio_url: `${envVarsTemp.virkailija_url}/organisaatio-service/rest/organisaatio/v4/`,
      viestintapalvelu_url: `${envVarsTemp.virkailija_url}/ryhmasahkoposti-service/email`,
      ehoks_url: `${envVarsTemp.virkailija_url}/ehoks-virkailija-backend/api/v1/`,
      cas_url: `${envVarsTemp.virkailija_url}/cas`,
      stage: envName
    };

    this.runtime = lambda.Runtime.JAVA_11;
  }

  getParameterFromSsm = (parameterName: string): string => {
    return ssm.StringParameter.valueForStringParameter(
        this,
        `/${this.envName}/services/heratepalvelu/${parameterName}`
    );
  };

  getEnvVarFromSsm = (parameterName: string): string => {
    return this.getParameterFromSsm(parameterName.replace("_", "-"));
  };
}
