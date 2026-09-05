# lab

End-to-end test for the `auto` exchange transport.

Not part of the test suite. There is no `add_test()`, nothing runs in ctest, and
nothing else in the tree depends on this directory. It needs a UCX listener and
a free port, which a unit test suite should not be arranging.

`TRANSPORT_DISCOVERY.md` is the working record behind the change: what was
tried, what was measured, and what turned out to be wrong.

## What it proves

`AutoTransportSelectionLab.cpp` runs the same query three times against
different conditions, with the UCX and fallback exchange source factories
registered together and nothing in the plan naming a transport:

| | |
|---|---|
| A | peer nobody has asked about → fallback carries it, a probe runs behind it |
| B | same peer, now known → **UCX carries it**, data crosses the wire |
| C | peer that never answers → fallback carries it, and keeps carrying it |

Throughout, the producer runs the **standard** `PartitionedOutput` into the
**stock** output buffer and the consumer runs a **plain** `exec::Exchange`. No
transport-specific operator, no output transport registry entry, and no
`transportKind` in the plan.

## Build and run

Everything happens inside the dev container:

```bash
scripts/docker/ucx-dev.sh enter
```

Configure once:

```bash
cmake -B _build/ucx-cpu -S . -GNinja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
  -DVELOX_BUILD_TESTING=ON \
  -DVELOX_BUILD_LAB=ON \
  -DVELOX_ENABLE_UCX_EXCHANGE=ON \
  -DVELOX_ENABLE_CUDF=OFF \
  -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
```

Then:

```bash
ninja -C _build/ucx-cpu velox_lab_transport_selection
UCX_CM_REUSEADDR=y ./_build/ucx-cpu/lab/velox_lab_transport_selection
```

`UCX_CM_REUSEADDR=y` lets the listener rebind while connections it accepted are
still in TIME_WAIT, so repeated runs on one port do not fail to bind.
`VELOX_UCX_LAB_PORT` overrides the port if something else wants it.

## Build configuration notes

`VELOX_ENABLE_UCX_EXCHANGE=ON` with `VELOX_ENABLE_CUDF=OFF` is deliberate. The
UCX exchange's CPU row path has no cuDF dependency, so the transport builds
without paying for a cuDF build. Turn cuDF on only when the GPU path is needed.

CMake 4 and `CMAKE_POLICY_VERSION_MINIMUM=3.5` are both required by the bundled
UCXX build, whose rapids-cmake declares `cmake_minimum_required(VERSION 4.0)`.
CMake 4 is baked into the dev container image.
