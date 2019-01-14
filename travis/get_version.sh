#!/usr/bin/env bash

# Finds the next version number, aborts if env.release is not set
user_version=$1
version=$(grep "version=" gradle.properties | cut -d'=' -f2)
[[ "$user_version" == "" ]] && echo "Release version not set in custom config" && travis_terminate 0
if [[ "$user_version" == "bugfix" ]]; then
    version=${version%-*}
elif [[ "$user_version" == "minor" ]]; then
    version=${version%%.*}.$((`echo $version | cut -d'.' -f 2`+1)).0
elif [[ "$user_version" == "major" ]]; then
    version=$((${version%%.*}+1)).0.0
elif [[ "$user_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    version=$user_version
else
    echo "Got unsupported version $user_version, use bugfix|minor|major|<version>" >&2
    exit 1
fi
echo $version