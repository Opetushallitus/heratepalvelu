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
(cd "${git_root}/cdk/" && npm install)
(cd "${git_root}/cdk/" && cdk --profile "oph-${account}" "$@")
