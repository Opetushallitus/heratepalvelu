#!/bin/bash

env=$1
stack=$2

test="sieni"
qa="pallero"
prod="sade"

if [ "$#" -eq 2 ]; then
  if lein test; then
    lein with-profile uberjar uberjar
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
    elif [ "$stack" == "tpk" ]; then
      if [ "$env" == "$prod" ]; then
        aws-vault exec oph-prod -- cdk deploy sade-services-heratepalvelu-tpk
      elif [ "$env" == "$qa" ]; then
        aws-vault exec oph-dev -- cdk deploy pallero-services-heratepalvelu-tpk
      elif [ "$env" == "$test" ]; then
        aws-vault exec oph-dev -- cdk deploy sieni-services-heratepalvelu-tpk
      else
        echo "hyväksytyt env parametrit: sieni, pallero, sade"
      fi
    elif [ "$stack" == "teprah" ]; then
      if [ "$env" == "$prod" ]; then
        aws-vault exec oph-prod -- cdk deploy sade-services-heratepalvelu-teprah
      elif [ "$env" == "$qa" ]; then
        aws-vault exec oph-dev -- cdk deploy pallero-services-heratepalvelu-teprah
      elif [ "$env" == "$test" ]; then
        aws-vault exec oph-dev -- cdk deploy sieni-services-heratepalvelu-teprah
      else
        echo "hyväksytyt env parametrit: sieni, pallero, sade"
      fi
    else
      echo "hyväksytyt stack parametrit: amis, tep, tpk, teprah"
    fi
  fi
else
  echo "parametrit <env> <stack>"
fi
