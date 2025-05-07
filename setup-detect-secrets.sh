#!/bin/bash

set -e
set -x

#
# Documentation for detect-secrets is here:
# https://github.com/IBM/detect-secrets/blob/master/docs/scan.md
#
function install_detect_secrets {
  # Detect secrets installation used for scanning.
  pip install --upgrade "git+https://github.com/ibm/detect-secrets.git@master#egg=detect-secrets"
  # For running the pre-commit hook.
  pip install pre-commit
  echo "Installing the pre-commit hook..."
  pre-commit install
}

install_detect_secrets

