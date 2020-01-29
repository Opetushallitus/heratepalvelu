#!/bin/bash

env=$1
qa="pallero"
prod="sade"

if lein test ; then
  lein uberjar
  cd cdk || exit $?
  if [ "$env" == "$prod" ]; then
    aws-vault exec oph-prod -- cdk deploy sade-services-heratepalvelu
  elif [ "$env" == "$qa" ]; then
    aws-vault exec oph-dev -- cdk deploy pallero-services-heratepalvelu
  else
    aws-vault exec oph-dev -- cdk deploy sieni-services-heratepalvelu
  fi
fi