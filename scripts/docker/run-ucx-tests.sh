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
# Runs the ucx_exchange_test binary with --velox_ucx_exchange (required --
# Communicator::initAndGet() is a no-op without it) and excludes the two
# parameterizations built from a numRowsPerChunk=1,000,000 base config
# ("Simple", "SourceDrivers") crossed with numUpstreamTasks=10 -- those push
# ~1 billion rows through the exchange at once, sized for a datacenter GPU
# (40GB+) rather than a desktop card. Every other parameterization in the
# suite tops out around 4 million rows. Pass extra gtest/gflags args to
# append to or override these (gflags/gtest use last-occurrence-wins, so a
# later --gtest_filter=... on the command line takes precedence).
#
# Usage:
#   scripts/docker/run-ucx-tests.sh [extra args...]

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_BIN="$REPO_ROOT/_build/release/velox/experimental/ucx-exchange/tests/ucx_exchange_test"

exec "$TEST_BIN" --velox_ucx_exchange \
  --gtest_filter='-*RowsPer1000000_Upstream10*' \
  "$@"
