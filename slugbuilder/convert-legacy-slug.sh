#!/bin/bash
#
# A script to convert a legacy slug tarball to a Flynn squashfs image

# exit on failure
set -eo pipefail

# create a tmp dir
tmp="$(mktemp --directory)"
trap "rm -rf ${tmp}" EXIT

# extract the slug tarball from stdin into an "app" directory
mkdir "${tmp}/app"
cat | tar xz -C "${tmp}/app"

# create an explicit user
export USER="flynn"
useradd \
  --comment "Flynn slug user" \
  --home    "/app" \
  --user-group \
  "${USER}"

# update file ownership
chown -R "${USER}:${USER}" "${tmp}/app"

# import user information
mkdir -p "${tmp}/etc"
cp "/etc/passwd" "${tmp}/etc/passwd"
cp "/etc/group" "${tmp}/etc/group"

# create the artifact
/bin/create-artifact \
  --dir "${tmp}" \
  --uid "$(id --user "${USER}")" \
  --gid "$(id --group "${USER}")"
