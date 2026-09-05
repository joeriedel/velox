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
# Persistent dev container for velox/experimental/ucx-exchange, built from
# scripts/docker/ucx-exchange-dev.dockerfile.
#
# Usage:
#   scripts/docker/ucx-dev.sh enter   # start the container if needed, attach a zsh shell
#   scripts/docker/ucx-dev.sh stop    # stop (and remove, via --rm) the container
#   scripts/docker/ucx-dev.sh build   # stop the container, then rebuild the image
#
# `enter` also installs this repo's pre-commit hook (git config
# core.hooksPath is unset, so this writes directly to .git/hooks/) using
# the container's pre-commit/clang-format/etc. This is deliberate: `git
# commit` from the host will fail (the hook hardcodes the container's
# python3, which the host doesn't have `pre-commit` installed for) --
# commit from inside the container.

set -euo pipefail

IMAGE=velox-ucx-dev
CONTAINER=velox-ucx-dev
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

function enter {
  if ! podman container exists "$CONTAINER"; then
    local dotfile_mounts=()
    # Mount the host's oh-my-zsh config and gitconfig (identity, aliases, ...)
    # read-only so the container shell matches your usual setup. Root's
    # $HOME in the container is /root, which is where oh-my-zsh's own
    # .zshrc -- and git -- expect to find these by default. The image
    # already sets safe.directory for /velox at the system config level
    # (see ucx-exchange-dev.dockerfile), since the repo is owned by your
    # host UID but the container runs as root.
    # .ssh is mounted read-only: your agent has no loaded identities (git
    # over SSH uses key files directly via the default IdentityFile
    # behavior), so the client needs the actual keys/config/known_hosts,
    # not just agent forwarding. File permissions carry over unchanged
    # through the bind mount, which is what ssh's strict permission
    # checks look at.
    for dotfile in .zshrc .oh-my-zsh .p10k.zsh .gitconfig .ssh; do
      if [ -e "$HOME/$dotfile" ]; then
        dotfile_mounts+=(-v "$HOME/$dotfile:/root/$dotfile:ro,Z")
      fi
    done

    # Persist ccache across container recreations. The container runs with
    # --rm, so anything written to the container's own filesystem -- including
    # ccache's default /root/.cache/ccache -- is discarded on stop. Velox
    # builds are expensive enough (a cuDF-enabled build fetches and compiles
    # rmm, kvikio and cuDF from source) that losing the cache every time is
    # costly. Mounting the host's cache directory keeps it, and shares it
    # across checkouts.
    mkdir -p "$HOME/.cache/ccache"
    local ccache_mount=(-v "$HOME/.cache/ccache:/root/.cache/ccache:Z")

    # Also forward the host's SSH agent, in case it does have identities
    # loaded in some other session.
    local ssh_mounts=()
    if [ -n "${SSH_AUTH_SOCK:-}" ] && [ -S "${SSH_AUTH_SOCK}" ]; then
      ssh_mounts+=(-v "$SSH_AUTH_SOCK:$SSH_AUTH_SOCK:Z" -e SSH_AUTH_SOCK="$SSH_AUTH_SOCK")
    fi

    # Also bind-mount the repo at its host-identical absolute path (in
    # addition to /velox). clangd running in-container (via
    # ucx-clangd-wrapper.sh) needs the file paths it sees to match the
    # paths the host-side editor opens, since compile_commands.json is
    # rewritten to use host paths -- see gen-compile-commands.sh.
    podman run -d --rm --name "$CONTAINER" --hostname ucx-dev \
      --device nvidia.com/gpu=all --security-opt=label=disable \
      -e DISABLE_AUTO_UPDATE=true \
      -v "$REPO_ROOT":/velox:Z -v "$REPO_ROOT":"$REPO_ROOT":Z -w /velox \
      "${dotfile_mounts[@]}" "${ssh_mounts[@]}" "${ccache_mount[@]}" \
      "$IMAGE" sleep infinity

    # The image only whitelists /velox as a safe.directory (see
    # ucx-exchange-dev.dockerfile); whitelist the host-path mount too so
    # git (and clangd's query-driver, which shells out to git) doesn't
    # balk at it.
    podman exec "$CONTAINER" git config --system --add safe.directory "$REPO_ROOT"

    # Activates the pre-commit hook (see .pre-commit-config.yaml and
    # velox/experimental/ucx-exchange/README.md's "Code Formatting"
    # section) so formatting runs on every commit. Writes to
    # .git/hooks/pre-commit in the bind-mounted repo, so unlike the
    # steps above this only needs to happen once per checkout -- it
    # persists on the host across container recreations -- but re-running
    # it here is cheap and keeps a fresh checkout working automatically.
    podman exec "$CONTAINER" pre-commit install
  fi
  exec podman exec -it -e TERM="$TERM" -e COLORTERM="${COLORTERM:-}" "$CONTAINER" zsh
}

function stop {
  if podman container exists "$CONTAINER"; then
    podman stop "$CONTAINER"
  else
    echo "$CONTAINER is not running."
  fi
}

function build {
  stop
  podman build -t "$IMAGE" -f "$REPO_ROOT"/scripts/docker/ucx-exchange-dev.dockerfile "$REPO_ROOT"
}

case "${1:-}" in
  enter) enter ;;
  stop) stop ;;
  build) build ;;
  *)
    echo "Usage: $0 {enter|stop|build}" >&2
    exit 1
    ;;
esac
