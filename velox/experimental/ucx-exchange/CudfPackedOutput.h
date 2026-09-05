/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

namespace facebook::velox::ucx_exchange {

/// Substitutes the cuDF packing output operator for exec::PartitionedOutput in
/// cuDF drivers. Idempotent.
///
/// UcxCudfDriverAdapter does the same substitution, but only for nodes whose
/// plan says transportKind=kUcx. Nothing here reads a transport out of the
/// plan, so that adapter never fires and this one does the work: it selects on
/// the plan being a cuDF plan instead.
///
/// Only active with VELOX_UCX_STOCK_OUTPUT_BUFFER set, which is what makes
/// UcxPartitionedOutput write to the ordinary output buffer rather than its
/// own queue.
void registerCudfPackedOutput();

} // namespace facebook::velox::ucx_exchange
