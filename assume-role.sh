#!/bin/bash -e

# This is the default terminal prompt on Ubuntu, you can get your default with 'echo $PS1'
DEFAULT_TERMINAL_PS="\[\e]0;\u@\h: \w\a\]${debian_chroot:+($debian_chroot)}\[\033[01;32m\]\u@\h\[\033[00m\]:\[\033[01;34m\]\w\[\033[00m\]\$ "
COLOR_RED="\[\033[1;31m\]"
COLOR_END="\[\033[0;00m\]"
USAGE_HELP="Usage:\n. ${BASH_SOURCE[0]} ROLE_TO_ASSUME\n. ${BASH_SOURCE[0]} deactivate"

# This script must be sourced as it manipulates environment variables
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "Error: Instead of running $0, you must source the script with a preceding '.' or 'source'" >&2
  echo -e $USAGE_HELP
  exit 1
fi

REQUESTED_ROLE=$1
if [ "$#" -ne 1 ]; then
  echo "Error: Invalid number of arguments"
  echo -e $USAGE_HELP
elif [ $REQUESTED_ROLE == "deactivate" ]; then
  unset AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_ACCESS_KEY_ID AWS_SECURITY_TOKEN ASSUMED_ROLE
  export PS1="$DEFAULT_TERMINAL_PS"
  echo "Assumed role deactivated"
else
  eval $(assume-role $REQUESTED_ROLE)
  export PS1="${COLOR_RED}${ASSUMED_ROLE}${COLOR_END} $DEFAULT_TERMINAL_PS"
  echo "Role $REQUESTED_ROLE is now active in this terminal"
fi
