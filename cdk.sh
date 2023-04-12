#!/usr/bin/env bash

environment="$1" && shift
test -z "$environment" \
&& echo "Specify environment (untuva, pallero, ...)" 1>&2 \
&& exit 1

git_root=$(git rev-parse --show-toplevel)
test -z "$git_root" \
&& echo "Run this script inside the heratepalvelu git repository" 1>&2 \
&& exit 1

case "$environment" in
	sade) account="prod" ;;
	sieni|hahtuva|untuva|pallero) account="dev" ;;
	sumu|utility) account="utility" ;;
	*)
		echo "Unknown environment: ${environment}"
		exit 1
esac

lein test || (echo "Fix tests first :)" 1>&2 && exit 1)
lein checkall || (echo "Fix static checks first :)" 1>&2 && exit 1)

lein with-profile uberjar uberjar
(cd "${git_root}/cdk/" && test ! -d node_modules && npm install)
(cd "${git_root}/cdk/" && npx aws-cdk --profile "oph-${account}" "$@")

### This is no longer needed as CDK can do federated MFA itself (with --profile)
#VENV="${git_root}/python-virtualenv" 
#test ! -d "$VENV" \
#&& python3 -m venv "$VENV" \
#&& "$VENV/bin/pip" install awscli
## Generate temporary aws credentials
#sts_session=$("$VENV/bin/aws" sts assume-role \
#	--role-arn "$role" \
#	--role-session-name "cdk-heratepalvelu-${environment}" \
#	--profile "oph-${account}")
#access_key_id=$(echo "${sts_session}" | \
#	jq -cr ".Credentials.AccessKeyId")
#secret_access_key=$(echo "${sts_session}" | \
#	jq -cr ".Credentials.SecretAccessKey")
#session_token=$(echo "${sts_session}" | \
#	jq -cr ".Credentials.SessionToken")

#&& AWS_ACCESS_KEY_ID=${access_key_id} \
#   AWS_SECRET_ACCESS_KEY=${secret_access_key} \
#   AWS_SESSION_TOKEN=${session_token} \

