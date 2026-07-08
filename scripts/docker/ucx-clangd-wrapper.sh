#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Drop-in stand-in for the `clangd` binary that runs the real clangd
# inside the velox-ucx-dev container instead of on the host -- the host
# has none of the toolchain (gcc-toolset-14, CUDA, UCX, folly, ...) that
# clangd needs to resolve. Point an editor's clangd binary path at this
# script; it forwards the editor's stdio straight through to the
# in-container clangd over `podman exec -i`, so from the editor's
# perspective it's talking to a normal local language server.
#
# Requires scripts/docker/ucx-dev.sh enter to have been run at least once
# (starts the persistent container this execs into) and
# scripts/docker/gen-compile-commands.sh to have been run (produces the
# host-path-rooted compile_commands.json clangd needs to find matching
# entries for files opened by their host path).

set -euo pipefail

CONTAINER=velox-ucx-dev

if ! podman container exists "$CONTAINER"; then
  echo "error: $CONTAINER is not running -- run scripts/docker/ucx-dev.sh enter first" >&2
  exit 1
fi

exec podman exec -i "$CONTAINER" clangd "$@"
