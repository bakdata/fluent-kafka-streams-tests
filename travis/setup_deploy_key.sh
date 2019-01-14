#!/usr/bin/env bash

# https://github.com/alrra/travis-scripts/blob/master/docs/github-deploy-keys.md
# Sets up SSH deploy key, note that private repos can add the key more easily
declare -r SSH_FILE="$(mktemp -u $HOME/.ssh/XXXXX)"
mv .ssh/* $SSH_FILE
chmod 600 "$SSH_FILE" \
  && printf "%s\n" \
      "Host github.com" \
      "  IdentityFile $SSH_FILE" \
      "  LogLevel ERROR" >> ~/.ssh/config