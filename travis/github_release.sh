#!/bin/bash

# override CHANGELOG temporarily for outputting a minimalistic changelog for github release
./gradlew generateChangelog -Pchangelog.releaseVersion=${TRAVIS_TAG} -Pchangelog.sinceTag=${TRAVIS_TAG}
body="$(cat CHANGELOG.md)"

id=$(curl -H "Authorization: token $GITHUB_TOKEN" "https://api.github.com/repos/$TRAVIS_REPO_SLUG/releases" | jq '.[] | select(.tag_name = "${TRAVIS_TAG}") | .id')

jq -n --arg body "$body" \
  '{
    body: $body
  }' > CHANGELOG.json

curl -X PATCH -H "Authorization: token $GITHUB_TOKEN" --data @CHANGELOG.json "https://api.github.com/repos/$TRAVIS_REPO_SLUG/releases/$id"