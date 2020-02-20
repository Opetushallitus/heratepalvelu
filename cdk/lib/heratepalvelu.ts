import { Construct, Stack, StackProps, Tag } from "@aws-cdk/core";
import ssm = require("@aws-cdk/aws-ssm");


export class HeratepalveluStack extends Stack {
  envName: string;
  envVars: object;

  constructor(scope: Construct,
              id: string,
              envName: string,
              version: string,
              props?: StackProps) {
    super(scope, id, props);

    Tag.add(this, "Deployed version", version);

    this.envName = envName;

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
      ehoks_url: `${envVarsTemp.virkailija_url}/ehoks-virkailija-backend/api/v1/`,
      viestintapalvelu_url: `${envVarsTemp.virkailija_url}/ryhmasahkoposti-service/email/`,
      cas_url: `${envVarsTemp.virkailija_url}/cas/`,
      stage: envName
    };
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

