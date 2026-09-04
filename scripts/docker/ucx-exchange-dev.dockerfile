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
# Local development container for velox/experimental/ucx-exchange.
#
# Builds on an NVIDIA CUDA devel image so the toolkit (nvcc, cudart, nvrtc,
# ...) is already present. We only layer Velox's own build dependencies
# (scripts/setup-centos9.sh) and a CUDA-aware UCX build
# (scripts/setup-centos-adapters.sh install_ucx) on top, mirroring what the
# "adapters" stage in centos-multi.dockerfile does for CI.
#
# Build:
#   podman build -t velox-ucx-dev -f scripts/docker/ucx-exchange-dev.dockerfile .
#
# For a persistent dev shell (zsh, GPU passthrough, repo mounted at
# /velox), use scripts/docker/ucx-dev.sh instead of invoking podman
# directly:
#   scripts/docker/ucx-dev.sh enter
#   scripts/docker/ucx-dev.sh stop

ARG CUDA_VERSION=12.9.1
FROM docker.io/nvidia/cuda:${CUDA_VERSION}-devel-rockylinux9

COPY scripts/setup-helper-functions.sh /
COPY scripts/setup-versions.sh /
COPY scripts/setup-common.sh /
COPY scripts/setup-centos9.sh /
COPY scripts/setup-centos-adapters.sh /
COPY CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch /
COPY CMake/resolve_dependency_modules/arrow/arrow-testing-boost.patch /
COPY CMake/resolve_dependency_modules/fbthrift/compactv1-protocol-refiller.patch /

# CMake 4.0 removed support for cmake minimums of <=3.5 and will fail builds;
# this overrides it. Matches centos-multi.dockerfile.
ENV CMAKE_POLICY_VERSION_MINIMUM="3.5" \
    VELOX_ARROW_CMAKE_PATCH="/cmake-compatibility.patch /arrow-testing-boost.patch" \
    VELOX_FBTHRIFT_CMAKE_PATCH="/compactv1-protocol-refiller.patch"

# setup-centos9.sh only auto-enables the CRB repo when /etc/os-release
# identifies the distro as CentOS; the nvidia base image is Rocky Linux, so
# enable it explicitly here. CRB carries gtest-devel, gmock-devel,
# libdwarf-devel, and ucx-devel's build-time siblings.
RUN dnf install -y dnf-plugins-core && \
    dnf config-manager --set-enabled crb

# The nvidia base image already has the CUDA 12.9 dnf repo enabled. CUDA
# 13.3 renamed cuda-cccl to cccl in that same repo, and the new package
# obsoletes the CUDA 12.9 package the preinstalled 12.9 devel packages
# still require -- a plain `dnf update -y` (run by
# install_build_prerequisites below) would otherwise fail on that
# obsoletion. Apply the same best=False workaround
# setup-centos-adapters.sh's configure_dnf_for_cuda uses.
RUN bash -c 'source /setup-centos-adapters.sh && configure_dnf_for_cuda'

# Installs build prerequisites (gcc-toolset-12/14, cmake, ninja, ccache, ...)
# and all Velox dependencies (folly, fbthrift, arrow, ...) to /usr/local.
RUN bash /setup-centos9.sh

# The repo is bind-mounted from the host (owned by the host UID) but the
# container runs as root, so git would otherwise refuse it as "dubious
# ownership". Set this at the system config level so it applies
# regardless of whether a host ~/.gitconfig is mounted over it (see
# scripts/docker/ucx-dev.sh).
RUN git config --system --add safe.directory /velox

# Builds UCX from source with CUDA support (--with-cuda=/usr/local/cuda,
# already present in this base image) to /usr/local, producing the
# ucx-config.cmake package find_package(ucx REQUIRED) needs.
ENV CUDA_VERSION=12.9
RUN bash /setup-centos-adapters.sh install_ucx && \
    dnf clean all

ENV PATH=/usr/local/cuda/bin:${PATH} \
    NVIDIA_VISIBLE_DEVICES=all \
    NVIDIA_DRIVER_CAPABILITIES=compute,utility

# CC/CXX default to gcc-toolset-12, matching the "centos9" CI image. The
# cuDF/UCX exchange build itself needs GCC >= 13.3 -- source
# gcc-toolset-14 and override CC=gcc CXX=g++ right before configuring
# that build, same as .github/workflows/linux-build-base.yml does.
ENV CC=/opt/rh/gcc-toolset-12/root/bin/gcc \
    CXX=/opt/rh/gcc-toolset-12/root/bin/g++

# zsh for interactive use (see scripts/docker/ucx-dev.sh). Interactive
# shells default straight to gcc-toolset-14 since everything built from
# here on (velox, cudf, ucx-exchange) needs GCC >= 13.3 -- the base
# dependencies below were already compiled with gcc-toolset-12.
#
# ncurses-term supplies terminfo entries beyond the handful Rocky ships by
# default (tmux-256color, xterm-kitty, alacritty, ...) and
# glibc-langpack-en gives a real UTF-8 locale -- both needed for oh-my-zsh
# themes (powerline/nerd-font glyphs, 256-color prompts) to render
# correctly over `ucx-dev.sh enter`.
#
# clang-tools-extra provides clangd, used for editor intellisense (see
# scripts/docker/ucx-clangd-wrapper.sh) against the compile_commands.json
# this environment's CMake configure produces.
RUN dnf install -y zsh ncurses-term glibc-langpack-en clang-tools-extra && \
    { \
      echo 'source /opt/rh/gcc-toolset-14/enable'; \
      echo 'export CC=gcc CXX=g++'; \
    } >>/etc/zshrc

# The pre-commit framework (see .pre-commit-config.yaml and
# velox/experimental/ucx-exchange/README.md's "Code Formatting" section)
# runs formatting/lint hooks on every commit. Installed globally (no
# --user) so the `pre-commit` entrypoint lands on the default PATH
# regardless of $HOME. Baked into the image, not installed at container
# runtime, since the container itself is ephemeral (`--rm`) -- only
# `pre-commit install`, run once per checkout against the bind-mounted
# repo, needs to happen at container-start time (see ucx-dev.sh), because
# that writes to the host repo's .git/hooks, which does persist.
RUN pip3 install pre-commit

ENV LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

WORKDIR /velox

ENTRYPOINT ["/bin/bash", "-c", "source /opt/rh/gcc-toolset-12/enable && exec \"$@\"", "--"]
CMD ["/bin/bash"]
