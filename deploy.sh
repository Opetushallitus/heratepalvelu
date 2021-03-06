#!/bin/bash

env=$1
stack=$2

test="sieni"
qa="pallero"
prod="sade"

if [ "$#" -eq 2 ]; then
  if lein test; then
    lein uberjar
    cd cdk || exit $?
    if [ "$stack" == "amis" ]; then
      if [ "$env" == "$prod" ]; then
        aws-vault exec oph-prod -- cdk deploy sade-services-heratepalvelu
      elif [ "$env" == "$qa" ]; then
        aws-vault exec oph-dev -- cdk deploy pallero-services-heratepalvelu
      elif [ "$env" == "$test" ]; then
        aws-vault exec oph-dev -- cdk deploy sieni-services-heratepalvelu
      else
        echo "hyväksytyt env parametrit: sieni, pallero, sade"
      fi
    elif [ "$stack" == "tep" ]; then
      if [ "$env" == "$prod" ]; then
        aws-vault exec oph-prod -- cdk deploy sade-services-heratepalvelu-tep
      elif [ "$env" == "$qa" ]; then
        aws-vault exec oph-dev -- cdk deploy pallero-services-heratepalvelu-tep
      elif [ "$env" == "$test" ]; then
        aws-vault exec oph-dev -- cdk deploy sieni-services-heratepalvelu-tep
      else
        echo "hyväksytyt env parametrit: sieni, pallero, sade"
      fi
    else
      echo "hyväksytyt stack parametrit: amis, tep"
    fi
  fi
else
  echo "parametrit <env> <stack>"
fi