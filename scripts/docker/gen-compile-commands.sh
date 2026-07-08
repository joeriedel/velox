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
# Rewrites _build/release/compile_commands.json (generated inside the
# ucx-dev container, so its "directory"/"file"/"command" fields are all
# rooted at /velox) into a copy rooted at this repo's host path instead,
# and drops it at the repo root where clangd auto-discovers it.
#
# This only rewrites paths -- it doesn't reconfigure or rebuild anything.
# Re-run it whenever the /velox-rooted compile_commands.json is
# regenerated (e.g. after a CMake reconfigure inside the container).
#
# Usage:
#   scripts/docker/gen-compile-commands.sh [build-dir]
#   (build-dir defaults to _build/release)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="${1:-$REPO_ROOT/_build/release}"
SRC="$BUILD_DIR/compile_commands.json"
DEST="$REPO_ROOT/compile_commands.json"

if [ ! -f "$SRC" ]; then
  echo "error: $SRC not found -- build inside the container first" >&2
  exit 1
fi

python3 -c "
import json, re, shlex, sys

repo_root = sys.argv[1]
src, dest = sys.argv[2], sys.argv[3]

# Velox's own source tree has a top-level directory literally named
# 'velox' (repo_root/velox/experimental/...), so inside the container a
# real path looks like /velox/velox/experimental/... -- and some CMake
# target names/object paths (e.g. .../CMakeFiles/velox_ucx_exchange.dir/...)
# contain '/velox' as a substring that isn't a path at all. Only translate
# a token when '/velox' is the mount-root prefix: i.e. it starts the
# token's path portion, optionally after a glued flag like -I/-isystem/
# -DNAME= that itself contains no slash.
_PATH_RE = re.compile(r'^(?P<prefix>[^/]*)(?P<path>/velox(?:/.*)?)\$')

def translate_token(token):
    match = _PATH_RE.match(token)
    if not match:
        return token
    return match.group('prefix') + repo_root + match.group('path')[len('/velox'):]

with open(src) as f:
    entries = json.load(f)

for entry in entries:
    for key in ('directory', 'file'):
        if key in entry:
            entry[key] = translate_token(entry[key])
    if 'command' in entry:
        tokens = [translate_token(tok) for tok in shlex.split(entry['command'])]
        entry['command'] = shlex.join(tokens)
    if 'arguments' in entry:
        entry['arguments'] = [translate_token(arg) for arg in entry['arguments']]

with open(dest, 'w') as f:
    json.dump(entries, f, indent=2)
" "$REPO_ROOT" "$SRC" "$DEST"

echo "wrote $DEST ($(python3 -c "import json; print(len(json.load(open('$DEST'))))") entries)"
