#!/bin/bash

# override CHANGELOG temporarily for outputting a minimalistic changelog for github release
./gradlew generateChangelog -Pchangelog.releaseVersion=${TRAVIS_TAG} -Pchangelog.sinceTag=${TRAVIS_TAG}
body="$(cat CHANGELOG.md)"

releases=$(curl -H "Authorization: token $GITHUB_TOKEN" "https://api.github.com/repos/$TRAVIS_REPO_SLUG/releases" | jq '.[] | {tag_name: .tag_name, id: .id}')
echo "Found the following releases $releases"
id=$(echo "${releases}" | jq "select(.tag_name == \"${TRAVIS_TAG}\") | .id")

jq -n --arg body "$body" \
  '{
    body: $body
  }' > CHANGELOG.json

echo "Setting body to release id $id: $body"
curl -X PATCH -H "Authorization: token $GITHUB_TOKEN" --data @CHANGELOG.json "https://api.github.com/repos/$TRAVIS_REPO_SLUG/releases/$id"