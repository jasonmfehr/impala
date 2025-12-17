#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Adds a tag to an existing multi-platform manifest. For example, when releasing a new
# image, use this script to add the "stable-prev" tag to the stable manifest and then use
# this script again to add the "stable" tag to the "latest" manifest.
#
# This script is agnostic about the underlying images and platforms in the source manifest
# and simply copies all existing manifest entries to the new manifest.
#
# Inputs (via environment variables):
#   OCI_IMG:      Base name of the OCI image repo used for all images and manifests.
#   OCI_SRC_TAG:  Source tag of the existing multi-platform manifest to copy.
#   OCI_DEST_TAG: New tag name for the multi-platform manifest.

set -euo pipefail

OCI_IMG="${OCI_IMG:-apache/impala-dev}"
OCI_SRC_TAG="${OCI_SRC_TAG:-stable}"
OCI_DEST_TAG="${OCI_DEST_TAG:-stable-prev}"

echo "Tagging '${OCI_IMG}:${OCI_SRC_TAG}' with new tag '${OCI_DEST_TAG}'"
echo

SRC_IMGS=()
for SHA in \
    $(docker manifest inspect "${OCI_IMG}:${OCI_SRC_TAG}" | \
    jq -r ".manifests[].digest"); do
  SRC_IMGS+=("${OCI_IMG}@${SHA}")
done

docker manifest rm "${OCI_IMG}:${OCI_DEST_TAG}" || true
docker manifest create "${OCI_IMG}:${OCI_DEST_TAG}" "${SRC_IMGS[@]}"
docker manifest push "${OCI_IMG}:${OCI_DEST_TAG}"
