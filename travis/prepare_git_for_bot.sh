#!/usr/bin/env bash

git config user.email "31185348+bakdata-bot@users.noreply.github.com"
git config user.name "bakdata-bot"
remote=$(git remote get-url origin)
git remote set-url origin "${remote/https\:\/\/github.com\//git@github.com\:}"
git checkout master >/dev/null