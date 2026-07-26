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

#include <atomic>
#include <stdexcept>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/DefaultOutputBufferManager.h"
#include "velox/exec/OutputTransportRegistry.h"
#include "velox/exec/PartitionedOutput.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

using namespace facebook::velox;

namespace facebook::velox::exec::test {
namespace {

// A CPU test transport double: the default output buffer manager registered
// under a distinct transport id (kTransport), so operator selection can be
// verified without a real second transport (e.g. UCX). Failure injection occurs
// after the default manager creates or updates real buffer state.
class TestTransportBufferManager : public DefaultOutputBufferManager {
 public:
  static constexpr std::string_view kTransport{"test-transport"};

  enum class FailurePoint {
    kNone,
    kInitialize,
    kUpdate,
  };

  explicit TestTransportBufferManager(
      FailurePoint failurePoint = FailurePoint::kNone)
      : DefaultOutputBufferManager(Options{}), failurePoint_(failurePoint) {}

  void initializeTask(
      std::shared_ptr<Task> task,
      core::PartitionedOutputNode::Kind kind,
      int numDestinations,
      int numDrivers) override {
    ++initializeCount;
    DefaultOutputBufferManager::initializeTask(
        std::move(task), kind, numDestinations, numDrivers);
    if (failurePoint_ == FailurePoint::kInitialize) {
      throw std::runtime_error("injected output buffer initialization failure");
    }
  }

  bool updateOutputBuffers(
      const std::string& taskId,
      int numDestinations,
      bool noMoreBuffers) override {
    ++updateCount;
    const auto updated = DefaultOutputBufferManager::updateOutputBuffers(
        taskId, numDestinations, noMoreBuffers);
    if (failurePoint_ == FailurePoint::kUpdate) {
      throw std::runtime_error("injected output buffer replay failure");
    }
    return updated;
  }

  void removeTask(const std::string& taskId) override {
    ++removeCount;
    DefaultOutputBufferManager::removeTask(taskId);
  }

  std::atomic<int> initializeCount{0};
  std::atomic<int> updateCount{0};
  std::atomic<int> removeCount{0};

 private:
  const FailurePoint failurePoint_;
};

class OutputTransportTest : public HiveConnectorTestBase {
 protected:
  std::shared_ptr<Task> makeBroadcastTask(
      const std::string& taskId,
      const std::string& transportType) {
    auto plan = PlanBuilder()
                    .tableScan(ROW({"c0"}, {BIGINT()}))
                    .partitionedOutputBroadcast({}, "Presto", transportType)
                    .planFragment();
    return Task::create(
        taskId,
        std::move(plan),
        0,
        core::QueryCtx::create(driverExecutor_.get()),
        Task::ExecutionMode::kParallel,
        exec::Consumer{});
  }

  void registerTestTransport(
      const std::string& transportType,
      const std::shared_ptr<TestTransportBufferManager>& manager,
      std::shared_ptr<std::atomic<int>> builderInvocations = nullptr) {
    OutputTransportRegistry::global().insert(
        transportType,
        OutputTransportEntry::make<TestTransportBufferManager>(
            manager,
            [builderInvocations](
                int32_t operatorId,
                DriverCtx* ctx,
                const std::shared_ptr<const core::PartitionedOutputNode>& node,
                bool eagerFlush,
                const std::shared_ptr<TestTransportBufferManager>& manager)
                -> std::unique_ptr<Operator> {
              if (builderInvocations != nullptr) {
                ++*builderInvocations;
              }
              return std::make_unique<PartitionedOutput>(
                  operatorId, ctx, node, eagerFlush, manager);
            }));
  }
};

TEST_F(OutputTransportTest, usesDefaultAfterRegistryClear) {
  // unregisterAll() resets the registry to its baseline -- the built-in
  // in-memory default -- so a default task still runs after a clear.
  OutputTransportRegistry::unregisterAll();

  auto plan = PlanBuilder()
                  .tableScan(ROW({"c0"}, {BIGINT()}))
                  .project({"c0 % 10"})
                  .partitionedOutputBroadcast({})
                  .planFragment();
  auto task = Task::create(
      "task-default-output-manager-after-registry-clear",
      plan,
      0,
      core::QueryCtx::create(driverExecutor_.get()),
      Task::ExecutionMode::kParallel,
      exec::Consumer{});

  task->start(1, 1);

  auto defaultEntry = OutputTransportRegistry::tryGet(
      std::string(core::TransportKind::kInMemory));
  EXPECT_NE(defaultEntry, nullptr);
  EXPECT_TRUE(task->updateOutputBuffers(10, true /*noMoreBuffers*/));

  task->requestCancel();
  waitForTaskCompletion(task.get());
}

TEST_F(OutputTransportTest, errorsOnUnregisteredTransport) {
  // A node naming a transport with no registered manager is a misconfiguration:
  // resolution fails fast rather than silently running over another transport.
  const std::string transportType{"ucx-unregistered"};
  auto plan = PlanBuilder()
                  .tableScan(ROW({"c0"}, {BIGINT()}))
                  .project({"c0 % 10"})
                  .partitionedOutputBroadcast({}, "Presto", transportType)
                  .planFragment();

  auto queryCtx = core::QueryCtx::create(driverExecutor_.get());
  EXPECT_EQ(OutputTransportRegistry::tryGet(transportType), nullptr);

  auto task = Task::create(
      "task-transport-unregistered",
      std::move(plan),
      0,
      queryCtx,
      Task::ExecutionMode::kParallel,
      exec::Consumer{});
  VELOX_ASSERT_THROW(
      task->start(1, 1), "No output buffer manager registered for transport");
}

TEST_F(OutputTransportTest, selectsOperatorByTransportKind) {
  OutputTransportRegistry::unregisterAll();

  const std::string transportType{TestTransportBufferManager::kTransport};
  // One entry pairing a test manager with an operator factory that records it
  // ran, then delegates to a real PartitionedOutput. Proves createDriver builds
  // the operator from the resolved manager's entry, without needing UCX.
  auto builderInvocations = std::make_shared<std::atomic<int>>(0);
  auto manager = std::make_shared<TestTransportBufferManager>();
  registerTestTransport(transportType, manager, builderInvocations);

  auto plan = PlanBuilder()
                  .tableScan(ROW({"c0"}, {BIGINT()}))
                  .project({"c0 % 10"})
                  .partitionedOutputBroadcast({}, "Presto", transportType)
                  .planFragment();

  auto queryCtx = core::QueryCtx::create(driverExecutor_.get());
  auto task = Task::create(
      "task-transport-selection",
      std::move(plan),
      0,
      queryCtx,
      Task::ExecutionMode::kParallel,
      exec::Consumer{});
  task->start(1, 1);

  // createDriver used the transport entry's factory, not the default path.
  EXPECT_EQ(builderInvocations->load(), 1);
  EXPECT_TRUE(task->updateOutputBuffers(10, true /*noMoreBuffers*/));

  task->requestCancel();
  waitForTaskCompletion(task.get());
  OutputTransportRegistry::unregisterAll();
}

TEST_F(OutputTransportTest, replaysTerminalBufferUpdateReceivedBeforeStart) {
  OutputTransportRegistry::unregisterAll();
  SCOPE_EXIT {
    OutputTransportRegistry::unregisterAll();
  };

  const std::string transportType{TestTransportBufferManager::kTransport};
  auto manager = std::make_shared<TestTransportBufferManager>();
  registerTestTransport(transportType, manager);
  auto task =
      makeBroadcastTask("task-buffer-update-before-start", transportType);

  constexpr int32_t kNumDestinations = 10;
  EXPECT_TRUE(task->updateOutputBuffers(
      kNumDestinations / 2, false /* noMoreBuffers */));
  EXPECT_TRUE(
      task->updateOutputBuffers(kNumDestinations, true /* noMoreBuffers */));
  EXPECT_FALSE(task->updateOutputBuffers(
      kNumDestinations + 1, false /* noMoreBuffers */));
  task->start(1, 1);

  auto stats = manager->stats(task->taskId());
  ASSERT_TRUE(stats.has_value());
  EXPECT_TRUE(stats->noMoreBuffers);
  EXPECT_EQ(stats->buffersStats.size(), kNumDestinations);

  task->requestCancel();
  waitForTaskCompletion(task.get());
}

TEST_F(
    OutputTransportTest,
    initializationFailureTerminatesTaskAndCleansBuffer) {
  OutputTransportRegistry::unregisterAll();
  SCOPE_EXIT {
    OutputTransportRegistry::unregisterAll();
  };

  const std::string transportType{"failing-output-initialize"};
  auto manager = std::make_shared<TestTransportBufferManager>(
      TestTransportBufferManager::FailurePoint::kInitialize);
  registerTestTransport(transportType, manager);
  auto task =
      makeBroadcastTask("task-output-initialize-failure", transportType);

  VELOX_ASSERT_THROW(
      task->start(1, 1), "injected output buffer initialization failure");

  EXPECT_EQ(task->state(), TaskState::kFailed);
  EXPECT_EQ(manager->initializeCount.load(), 1);
  EXPECT_EQ(manager->updateCount.load(), 0);
  EXPECT_EQ(manager->removeCount.load(), 1);
  EXPECT_FALSE(manager->stats(task->taskId()).has_value());
  EXPECT_TRUE(task->outputBufferManager().expired());
  EXPECT_FALSE(task->updateOutputBuffers(1, true));
}

TEST_F(OutputTransportTest, replayFailureTerminatesTaskAndCleansBuffer) {
  OutputTransportRegistry::unregisterAll();
  SCOPE_EXIT {
    OutputTransportRegistry::unregisterAll();
  };

  const std::string transportType{"failing-output-replay"};
  auto manager = std::make_shared<TestTransportBufferManager>(
      TestTransportBufferManager::FailurePoint::kUpdate);
  registerTestTransport(transportType, manager);
  auto task = makeBroadcastTask("task-output-replay-failure", transportType);
  EXPECT_TRUE(task->updateOutputBuffers(10, true));

  VELOX_ASSERT_THROW(
      task->start(1, 1), "injected output buffer replay failure");

  EXPECT_EQ(task->state(), TaskState::kFailed);
  EXPECT_EQ(manager->initializeCount.load(), 1);
  EXPECT_EQ(manager->updateCount.load(), 1);
  EXPECT_EQ(manager->removeCount.load(), 1);
  EXPECT_FALSE(manager->stats(task->taskId()).has_value());
  EXPECT_TRUE(task->outputBufferManager().expired());
  EXPECT_FALSE(task->updateOutputBuffers(10, true));
}

} // namespace
} // namespace facebook::velox::exec::test
