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
#include "velox/experimental/ucx-exchange/UcxOutputQueueManager.h"
#include <cudf/column/column_factories.hpp>
#include <cudf/contiguous_split.hpp>
#include <cudf/table/table.hpp>
#include <cudf/types.hpp>
#include <folly/Executor.h>
#include <folly/synchronization/EventCount.h>
#include <gtest/gtest.h>
#include <rmm/device_buffer.hpp>
#include <atomic>
#include <cstring>
#include <memory>
#include <vector>
#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/experimental/ucx-exchange/UcxExchangeProtocol.h"
#include "velox/experimental/ucx-exchange/tests/UcxTestHelpers.h"

using namespace facebook::velox::ucx_exchange;
using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::core;

class UcxOutputQueueManagerTest : public testing::Test {
 protected:
  UcxOutputQueueManagerTest() {}

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = facebook::velox::memory::memoryManager()->addLeafPool();
    queueManager_ = std::make_shared<UcxOutputQueueManager>();
  }

  std::shared_ptr<Task> initializeTask(
      std::string_view taskId,
      int numDestinations,
      int numDrivers,
      core::PartitionedOutputNode::Kind kind =
          core::PartitionedOutputNode::Kind::kPartitioned) {
    auto task = createSourceTask(taskId, pool_, UcxTestData::kTestRowType);

    queueManager_->initializeTask(task, kind, numDestinations, numDrivers);
    return task;
  }

  std::unique_ptr<cudf::packed_columns> makePackedColumns(std::size_t numRows) {
    rmm::cuda_stream_view stream = rmm::cuda_stream_default;
    // Create table directly without going through pack/unpack
    auto table = facebook::velox::ucx_exchange::makeTable(
        numRows, UcxTestData::kTestRowType, stream);
    auto cols = std::make_unique<cudf::packed_columns>(
        cudf::pack(table->view(), stream));
    stream.synchronize();
    return cols;
  }

  void enqueue(std::string_view taskId, vector_size_t size) {
    enqueue(taskId, 0, size);
  }

  // Returns the enqueued page byte size.
  void enqueue(std::string_view taskId, int destination, vector_size_t size) {
    auto data = makePackedColumns(size);
    queueManager_->enqueue(taskId, destination, std::move(data), size);
  }

  void noMoreData(std::string_view taskId) {
    queueManager_->noMoreData(taskId);
  }

  void fetch(
      std::string_view taskId,
      int destination,
      bool expectedEndMarker = false) {
    bool receivedData = false;
    queueManager_->getData(
        taskId,
        destination,
        [destination, expectedEndMarker, &receivedData](
            std::shared_ptr<UcxGpuPayload> data,
            std::vector<int64_t> remainingBytes) {
          ASSERT_EQ(expectedEndMarker, data == nullptr)
              << "for destination " << destination;
          receivedData = true;
        });
    ASSERT_TRUE(receivedData) << "for destination " << destination;
  }

  UcxDataAvailableCallback receiveEndMarker(
      int destination,
      bool& receivedEndMarker) {
    return [destination, &receivedEndMarker](
               std::shared_ptr<UcxGpuPayload> data,
               std::vector<int64_t> remainingBytes) {
      EXPECT_FALSE(receivedEndMarker) << "for destination " << destination;
      EXPECT_TRUE(data == nullptr) << "for destination " << destination;
      EXPECT_TRUE(remainingBytes.empty());
      receivedEndMarker = true;
    };
  }

  void fetchEndMarker(std::string_view taskId, int destination) {
    bool receivedData = false;
    queueManager_->getData(
        taskId, destination, receiveEndMarker(destination, receivedData));
    EXPECT_TRUE(receivedData) << "for destination " << destination;
    queueManager_->deleteResults(taskId, destination);
  }

  void deleteResults(std::string_view taskId, int destination) {
    queueManager_->deleteResults(taskId, destination);
  }

  void registerForEndMarker(
      std::string_view taskId,
      int destination,
      bool& receivedEndMarker) {
    receivedEndMarker = false;
    queueManager_->getData(
        taskId, destination, receiveEndMarker(destination, receivedEndMarker));
    EXPECT_FALSE(receivedEndMarker) << "for destination " << destination;
  }

  UcxDataAvailableCallback receiveData(int destination, bool& receivedData) {
    receivedData = false;
    return [destination, &receivedData](
               std::shared_ptr<UcxGpuPayload> data,
               std::vector<int64_t> /*remainingBytes*/) {
      EXPECT_FALSE(receivedData) << "for destination " << destination;
      EXPECT_TRUE(data != nullptr) << "for destination " << destination;
      receivedData = true;
    };
  }

  void registerForData(
      std::string_view taskId,
      int destination,
      bool& receivedData) {
    receivedData = false;
    queueManager_->getData(
        taskId, destination, receiveData(destination, receivedData));
    EXPECT_FALSE(receivedData) << "for destination " << destination;
  }

  void dataFetcher(
      std::string_view taskId,
      int destination,
      int64_t& fetchedPackedColumns,
      bool earlyTermination) {
    int64_t received{0};
    folly::Random::DefaultGenerator rng;
    rng.seed(destination);
    while (true) {
      if (earlyTermination && folly::Random().oneIn(200)) {
        queueManager_->deleteResults(taskId, destination);
        return;
      }
      bool atEnd{false};
      folly::EventCount dataWait;
      auto dataWaitKey = dataWait.prepareWait();
      queueManager_->getData(
          taskId,
          destination,
          [&](std::shared_ptr<UcxGpuPayload> data,
              std::vector<int64_t> /*remainingBytes*/) {
            if (data == nullptr) {
              atEnd = true;
            } else {
              received++;
            }
            dataWait.notify();
          });
      dataWait.wait(dataWaitKey);
      if (atEnd) {
        break;
      }
    }
    queueManager_->deleteResults(taskId, destination);
    // out of order requests are allowed (fetch after delete)
    {
      struct Response {
        std::shared_ptr<UcxGpuPayload> data;
        std::vector<int64_t> remainingBytes;
      };
      folly::Promise<Response> promise;
      auto future = promise.getSemiFuture();
      queueManager_->getData(
          taskId,
          destination,
          [&promise](
              std::shared_ptr<UcxGpuPayload> data,
              std::vector<int64_t> remainingBytes) {
            promise.setValue(
                Response{std::move(data), std::move(remainingBytes)});
          });
      future.wait();
      ASSERT_TRUE(future.isReady());
      auto& response = future.value();
      ASSERT_EQ(response.remainingBytes.size(), 0);
      ASSERT_EQ(response.data, nullptr);
    }

    fetchedPackedColumns = received;
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::shared_ptr<UcxOutputQueueManager> queueManager_;
};

class TestExchangeServerLifecycle : public UcxExchangeServerLifecycle {
 public:
  TestExchangeServerLifecycle(std::string taskId, uint32_t destination)
      : key_{std::move(taskId), destination} {}

  const PartitionKey& getPartitionKey() const override {
    return key_;
  }

  void requestAbort() override {
    bool expected = false;
    if (abortRequested_.compare_exchange_strong(expected, true)) {
      ++abortRequests_;
    }
  }

  bool activate() override {
    if (closed_.load() || abortRequested_.load()) {
      return false;
    }
    activated_.store(true);
    return true;
  }

  bool isClosed() const override {
    return closed_.load();
  }

  int abortRequests() const {
    return abortRequests_.load();
  }

  bool isActivated() const {
    return activated_.load();
  }

 private:
  const PartitionKey key_;
  std::atomic<int> abortRequests_{0};
  std::atomic<bool> abortRequested_{false};
  std::atomic<bool> closed_{false};
  std::atomic<bool> activated_{false};
};

using ExchangeServerAdmission = UcxOutputQueueManager::ExchangeServerAdmission;

TEST(GpuHandshakeResponseTest, admissionStatusPreservesWireLayout) {
  HandshakeResponse accepted;
  EXPECT_FALSE(accepted.isIntraNodeTransfer);
  EXPECT_EQ(accepted.status, GpuHandshakeResponseStatus::kAccepted);

  HandshakeResponse rejected;
  rejected.status = GpuHandshakeResponseStatus::kTaskRemoved;

  EXPECT_EQ(sizeof(rejected), sizeof(accepted));
  EXPECT_EQ(rejected.status, GpuHandshakeResponseStatus::kTaskRemoved);
}

TEST(GpuAbortProtocolTest, usesDedicatedTaskScopedControlTags) {
  constexpr uint32_t kTaskHash = 0xa1b2c3d4;
  const auto ackTag = getGpuHandshakeAckTag(kTaskHash);
  const auto abortTag = getGpuAbortTag(kTaskHash);

  EXPECT_EQ(ackTag >> 32, kTaskHash);
  EXPECT_EQ(ackTag & 0xff000000ULL, GPU_HANDSHAKE_ACK_TAG);
  EXPECT_EQ(ackTag & 0x00ffffffULL, 0);
  EXPECT_EQ(abortTag >> 32, kTaskHash);
  EXPECT_EQ(abortTag & 0xff000000ULL, GPU_ABORT_TAG);
  EXPECT_EQ(abortTag & 0x00ffffffULL, 0);
  EXPECT_NE(abortTag, ackTag);
  EXPECT_NE(ackTag, getHandshakeAckTag(kTaskHash));
  EXPECT_NE(abortTag, getCpuRowAbortTag(kTaskHash));
  EXPECT_NE(abortTag, getMetadataTag(kTaskHash, 0));
  EXPECT_NE(abortTag, getDataTag(kTaskHash, 0));

  GpuHandshakeAckHeader ack;
  EXPECT_EQ(ack.magic, kGpuHandshakeAckMagic);
  EXPECT_EQ(ack.version, kGpuHandshakeAckVersion);
  EXPECT_EQ(ack.headerSize, sizeof(GpuHandshakeAckHeader));

  GpuAbortHeader abort;
  EXPECT_EQ(abort.magic, kGpuAbortMagic);
  EXPECT_EQ(abort.version, kGpuAbortVersion);
  EXPECT_EQ(abort.headerSize, sizeof(GpuAbortHeader));
}

TEST(GpuMetadataProtocolTest, preservesLogicalRowCount) {
  MetadataMsg metadata;
  metadata.cudfMetadata = std::make_unique<std::vector<uint8_t>>(
      std::initializer_list<uint8_t>{1, 2, 3, 4});
  metadata.dataSizeBytes = 37;
  metadata.numRows = 123;
  metadata.remainingBytes = {19, 7};
  metadata.atEnd = false;

  auto [serialized, size] = metadata.serialize();
  EXPECT_EQ(size, metadata.getSerializedSize());

  auto decoded = MetadataMsg::deserializeMetadataMsg(serialized.get());
  EXPECT_EQ(*decoded.cudfMetadata, *metadata.cudfMetadata);
  EXPECT_EQ(decoded.dataSizeBytes, metadata.dataSizeBytes);
  EXPECT_EQ(decoded.numRows, metadata.numRows);
  EXPECT_EQ(decoded.remainingBytes, metadata.remainingBytes);
  EXPECT_EQ(decoded.atEnd, metadata.atEnd);

  // Older workers end the message immediately after atEnd. New readers keep
  // accepting that layout and infer the count from column-bearing tables.
  std::vector<uint8_t> legacy(
      serialized.get(), serialized.get() + size - sizeof(WireRowCountType));
  const auto legacySize = static_cast<uint32_t>(legacy.size());
  std::memcpy(
      legacy.data() + sizeof(kMagicNumber), &legacySize, sizeof(legacySize));
  auto legacyDecoded = MetadataMsg::deserializeMetadataMsg(legacy.data());
  EXPECT_EQ(legacyDecoded.numRows, -1);
  EXPECT_EQ(legacyDecoded.dataSizeBytes, metadata.dataSizeBytes);
  EXPECT_EQ(legacyDecoded.remainingBytes, metadata.remainingBytes);
  EXPECT_EQ(legacyDecoded.atEnd, metadata.atEnd);
}

TEST_F(UcxOutputQueueManagerTest, preservesLogicalRowCount) {
  const std::string taskId = "logical-row-count";
  constexpr int32_t kNumRows = 17;
  initializeTask(taskId, 1, 1);

  auto emptyTable = std::make_unique<cudf::table>();
  auto packed = std::make_unique<cudf::packed_columns>(
      cudf::pack(emptyTable->view(), rmm::cuda_stream_default));
  rmm::cuda_stream_default.synchronize();
  queueManager_->enqueue(taskId, 0, std::move(packed), kNumRows);

  std::shared_ptr<UcxGpuPayload> received;
  queueManager_->getData(
      taskId,
      0,
      [&received](
          std::shared_ptr<UcxGpuPayload> data,
          std::vector<int64_t> /*remainingBytes*/) {
        received = std::move(data);
      });

  ASSERT_NE(received, nullptr);
  EXPECT_EQ(received->numRows, kNumRows);
  const auto bytes = received->data->gpu_data->size();
  received.reset();
  queueManager_->releaseInFlightBytes(taskId, 0, bytes, 1);
  noMoreData(taskId);
  fetchEndMarker(taskId, 0);
  queueManager_->removeTask(taskId);
}

TEST_F(
    UcxOutputQueueManagerTest,
    abnormalTaskRemovalAbortsAllAndOnlyMatchingServers) {
  auto taskA = initializeTask(
      "gpu-aborted-task-a", 2 /* numDestinations */, 1 /* numDrivers */);
  auto taskB = initializeTask(
      "gpu-unrelated-task-b", 1 /* numDestinations */, 1 /* numDrivers */);

  auto a0 = std::make_shared<TestExchangeServerLifecycle>(taskA->taskId(), 0);
  auto a0Duplicate =
      std::make_shared<TestExchangeServerLifecycle>(taskA->taskId(), 0);
  auto a1 = std::make_shared<TestExchangeServerLifecycle>(taskA->taskId(), 1);
  auto b0 = std::make_shared<TestExchangeServerLifecycle>(taskB->taskId(), 0);

  EXPECT_EQ(
      queueManager_->registerExchangeServer(a0),
      ExchangeServerAdmission::kAccepted);
  EXPECT_EQ(
      queueManager_->registerExchangeServer(a0Duplicate),
      ExchangeServerAdmission::kDuplicateServer);
  EXPECT_EQ(
      queueManager_->registerExchangeServer(a1),
      ExchangeServerAdmission::kAccepted);
  EXPECT_EQ(
      queueManager_->registerExchangeServer(b0),
      ExchangeServerAdmission::kAccepted);
  EXPECT_TRUE(a0->isActivated());
  EXPECT_FALSE(a0Duplicate->isActivated());
  EXPECT_TRUE(a1->isActivated());
  EXPECT_TRUE(b0->isActivated());
  EXPECT_EQ(a0Duplicate->abortRequests(), 1);

  taskA->requestAbort().wait();
  queueManager_->removeTask(taskA->taskId());

  EXPECT_EQ(a0->abortRequests(), 1);
  EXPECT_EQ(a0Duplicate->abortRequests(), 1);
  EXPECT_EQ(a1->abortRequests(), 1);
  EXPECT_EQ(b0->abortRequests(), 0);

  // Repeated removal preserves the tombstone and does not reissue aborts.
  queueManager_->removeTask(taskA->taskId());
  EXPECT_EQ(a0->abortRequests(), 1);
  EXPECT_EQ(a1->abortRequests(), 1);

  auto late = std::make_shared<TestExchangeServerLifecycle>(taskA->taskId(), 0);
  EXPECT_EQ(
      queueManager_->registerExchangeServer(late),
      ExchangeServerAdmission::kTaskRemoved);
  EXPECT_EQ(late->abortRequests(), 1);

  queueManager_->unregisterExchangeServer(b0);
  taskB->requestAbort().wait();
  queueManager_->removeTask(taskB->taskId());
  EXPECT_EQ(b0->abortRequests(), 0);
}

TEST_F(
    UcxOutputQueueManagerTest,
    finishedTaskServersDrainInsteadOfBeingAborted) {
  const std::string taskId = "gpu-normally-finished-task";
  auto task =
      initializeTask(taskId, 1 /* numDestinations */, 1 /* numDrivers */);
  auto active = std::make_shared<TestExchangeServerLifecycle>(taskId, 0);
  EXPECT_EQ(
      queueManager_->registerExchangeServer(active),
      ExchangeServerAdmission::kAccepted);

  task->testingFinish();
  queueManager_->removeTask(taskId);
  EXPECT_EQ(active->abortRequests(), 0);

  auto late = std::make_shared<TestExchangeServerLifecycle>(taskId, 0);
  EXPECT_EQ(
      queueManager_->registerExchangeServer(late),
      ExchangeServerAdmission::kTaskRemoved);
  EXPECT_FALSE(late->isActivated());
  EXPECT_EQ(late->abortRequests(), 1);

  auto reusedTask = createSourceTask(taskId, pool_, UcxTestData::kTestRowType);
  EXPECT_ANY_THROW(queueManager_->initializeTask(
      reusedTask,
      core::PartitionedOutputNode::Kind::kPartitioned,
      1 /* numDestinations */,
      1 /* numDrivers */));
  reusedTask->requestAbort().wait();
}

TEST_F(
    UcxOutputQueueManagerTest,
    removedTaskTombstoneRejectsLateServerAndReinitialization) {
  const std::string taskId = "gpu-removed-before-initialization";

  queueManager_->removeTask(taskId);
  queueManager_->removeTask(taskId);

  auto late = std::make_shared<TestExchangeServerLifecycle>(taskId, 0);
  EXPECT_EQ(
      queueManager_->registerExchangeServer(late),
      ExchangeServerAdmission::kTaskRemoved);
  EXPECT_EQ(late->abortRequests(), 1);

  auto task = createSourceTask(taskId, pool_, UcxTestData::kTestRowType);
  EXPECT_ANY_THROW(queueManager_->initializeTask(
      task,
      core::PartitionedOutputNode::Kind::kPartitioned,
      1 /* numDestinations */,
      1 /* numDrivers */));
  task->requestAbort().wait();
}

TEST_F(UcxOutputQueueManagerTest, serverMayArriveBeforeTaskInitialization) {
  const std::string taskId = "gpu-server-before-task-initialization";
  auto early = std::make_shared<TestExchangeServerLifecycle>(taskId, 0);
  EXPECT_EQ(
      queueManager_->registerExchangeServer(early),
      ExchangeServerAdmission::kAccepted);
  EXPECT_TRUE(early->isActivated());

  auto task =
      initializeTask(taskId, 1 /* numDestinations */, 1 /* numDrivers */);
  EXPECT_EQ(early->abortRequests(), 0);

  task->requestAbort().wait();
  queueManager_->removeTask(taskId);
  EXPECT_EQ(early->abortRequests(), 1);
}

TEST_F(UcxOutputQueueManagerTest, diagnostics) {
  const std::string taskId = "diagnostics";
  initializeTask(taskId, 2 /* numDestinations */, 1 /* numDrivers */);

  const auto diagnostics = queueManager_->toString(taskId);
  EXPECT_NE(
      diagnostics.find("UcxOutputQueue task=" + taskId), std::string::npos);
  EXPECT_NE(diagnostics.find("queues="), std::string::npos);
  EXPECT_NE(diagnostics.find("numFinished=0/1"), std::string::npos);

  queueManager_->removeTask(taskId);
  EXPECT_EQ(
      queueManager_->toString(taskId),
      "UcxOutputQueue[" + taskId + " not found]");
}

TEST_F(UcxOutputQueueManagerTest, basicPartitioned) {
  vector_size_t size = 100;
  std::string taskId = "basic-partitioned";
  auto task = initializeTask(taskId, 5 /* numDestinations*/, 1 /*numDrivers*/);

  ASSERT_FALSE(queueManager_->isFinished(taskId));

  // - enqueue one group per destination
  // - fetch and ask one group per destination
  // - enqueue one more group for destinations 0-2
  // - fetch and ask one group for destinations 0-2
  // - register end-marker callback for destination 3 and data callback for 4
  // - enqueue data for 4 and assert callback was called
  // - enqueue end marker for all destinations
  // - assert callback was called for destination 3
  // - fetch end markers for destinations 0-2 and 4

  for (int destination = 0; destination < 5; ++destination) {
    enqueue(taskId, destination, size);
  }
  EXPECT_FALSE(queueManager_->isFinished(taskId));

  for (int destination = 0; destination < 5; destination++) {
    fetch(taskId, destination);
  }

  EXPECT_FALSE(queueManager_->isFinished(taskId));

  for (int destination = 0; destination < 3; destination++) {
    enqueue(taskId, destination, size);
  }

  for (int destination = 0; destination < 3; destination++) {
    fetch(taskId, destination);
  }

  // destinations 0-2 received 2 packed_columns each
  // destinations 3-4 received 1 packed_columns each

  bool receivedEndMarker3;
  registerForEndMarker(taskId, 3, receivedEndMarker3);

  bool receivedData4;
  registerForData(taskId, 4, receivedData4);

  enqueue(taskId, 4, size);
  EXPECT_TRUE(receivedData4);

  noMoreData(taskId);
  EXPECT_TRUE(receivedEndMarker3);
  EXPECT_FALSE(queueManager_->isFinished(taskId));

  for (int destination = 0; destination < 3; destination++) {
    fetchEndMarker(taskId, destination);
  }

  EXPECT_TRUE(task->isRunning());

  deleteResults(taskId, 3);
  fetchEndMarker(taskId, 4);
  EXPECT_TRUE(queueManager_->isFinished(taskId));

  queueManager_->removeTask(taskId);
  EXPECT_TRUE(task->isFinished());
}

TEST_F(UcxOutputQueueManagerTest, basicAsyncFetch) {
  const vector_size_t size = 10;
  const std::string taskId = "basic-async-fetch";
  int numPartitions = 1;
  bool earlyTermination = false;

  initializeTask(taskId, numPartitions, 1);

  // asynchronously fetch data.
  int64_t fetchedPackedColumns = 0;
  std::thread thread([&]() {
    dataFetcher(taskId, 0, fetchedPackedColumns, earlyTermination);
  });
  const int totalPackedColumns = 100;
  int64_t producedPackedColumns = 0;

  for (int i = 0; i < totalPackedColumns; ++i) {
    const int partition = 0;
    try {
      enqueue(taskId, partition, size);
    } catch (...) {
      // Early termination might cause the task to fail early.
      ASSERT_TRUE(earlyTermination);
      break;
    }
    ++producedPackedColumns;
    if (folly::Random().oneIn(4)) {
      std::this_thread::sleep_for(std::chrono::microseconds(5)); // NOLINT
    }
  }
  noMoreData(taskId);

  thread.join();

  if (!earlyTermination) {
    ASSERT_EQ(fetchedPackedColumns, producedPackedColumns);
  }
  queueManager_->removeTask(taskId);
}

TEST_F(UcxOutputQueueManagerTest, lateTaskCreation) {
  const vector_size_t size = 10;
  const std::string taskId = "late-task-creation";
  int numPartitions = 1;
  bool earlyTermination = false;
  int destination = 0;

  // Fetch data from a non-existing task.
  struct Response {
    std::shared_ptr<UcxGpuPayload> data;
    std::vector<int64_t> remainingBytes;
  };
  folly::Promise<Response> promise;
  auto future = promise.getSemiFuture();
  queueManager_->getData(
      taskId,
      destination,
      [&promise](
          std::shared_ptr<UcxGpuPayload> data,
          std::vector<int64_t> remainingBytes) {
        promise.setValue(Response{std::move(data), std::move(remainingBytes)});
      });
  // initialize task.
  auto task = initializeTask(taskId, numPartitions, 1);
  // enqueue some data.
  enqueue(taskId, destination, size);
  noMoreData(taskId);
  uint32_t i = 0;
  while (true) {
    ++i;
    future.wait();
    ASSERT_TRUE(future.isReady());
    Response& response = future.value();
    if (!response.data) {
      // nullptr -> end of transmission.
      break;
    }
    folly::Promise<Response> promise;
    future = promise.getSemiFuture();
    queueManager_->getData(
        taskId,
        destination,
        [&promise](
            std::shared_ptr<UcxGpuPayload> data,
            std::vector<int64_t> remainingBytes) {
          promise.setValue(
              Response{std::move(data), std::move(remainingBytes)});
        });
  }
  ASSERT_EQ(i, 2);

  queueManager_->deleteResults(taskId, destination);
  queueManager_->removeTask(taskId);
  EXPECT_TRUE(task->isFinished());
}

TEST_F(UcxOutputQueueManagerTest, multiFetchers) {
  const std::vector<bool> earlyTerminations = {false, true};
  for (const auto earlyTermination : earlyTerminations) {
    SCOPED_TRACE(fmt::format("earlyTermination {}", earlyTermination));

    const vector_size_t size = 10;
    const std::string taskId =
        earlyTermination ? "multi-fetchers-early" : "multi-fetchers-complete";
    int numPartitions = 10;
    initializeTask(taskId, numPartitions, 1);

    // create one thread per partition to asynchronously fetch data.
    std::vector<std::thread> threads;
    std::vector<int64_t> fetchedPackedColumns(numPartitions, 0);

    for (size_t i = 0; i < numPartitions; ++i) {
      threads.emplace_back([&, i]() {
        dataFetcher(taskId, i, fetchedPackedColumns.at(i), earlyTermination);
      });
    }

    // enqueue packed columns.
    folly::Random::DefaultGenerator rng;
    rng.seed(1234);
    const int totalPackedColumns = 1000;
    std::vector<int64_t> producedPackedColumns(numPartitions, 0);
    for (int i = 0; i < totalPackedColumns; ++i) {
      // randomly chose a partition.
      const int partition = folly::Random().rand32(rng) % numPartitions;
      try {
        enqueue(taskId, partition, size);
      } catch (...) {
        // Early termination might cause the task to fail early.
        ASSERT_TRUE(earlyTermination);
        break;
      }
      ++producedPackedColumns[partition];
      if (folly::Random().oneIn(4)) {
        std::this_thread::sleep_for(std::chrono::microseconds(5)); // NOLINT
      }
    }
    noMoreData(taskId);

    for (int i = 0; i < threads.size(); ++i) {
      threads[i].join();
    }

    if (!earlyTermination) {
      for (int i = 0; i < numPartitions; ++i) {
        ASSERT_EQ(fetchedPackedColumns[i], producedPackedColumns[i]);
      }
    }
    queueManager_->removeTask(taskId);
  }
}

// Test BUG B scenario (a): Producer never starts / never calls
// initializeTask(). getData() creates a stub queue, then removeTask() is
// called without initializeTask() ever being called. The pending callback
// must fire with nullptr to unblock the consumer.
TEST_F(UcxOutputQueueManagerTest, callbackFiredOnTerminateBeforeInit) {
  const std::string taskId = "orphanTest";

  bool callbackFired = false;
  bool receivedNullptr = false;
  queueManager_->getData(
      taskId,
      0, // destination
      [&callbackFired, &receivedNullptr](
          std::shared_ptr<UcxGpuPayload> data,
          std::vector<int64_t> remainingBytes) {
        callbackFired = true;
        receivedNullptr = (data == nullptr);
      });

  // Callback should NOT have fired yet (queue is empty, no data).
  EXPECT_FALSE(callbackFired);

  // Now simulate the producer failing: removeTask fires terminate().
  // The stub queue has task_ == nullptr so the isRunning() check is skipped.
  queueManager_->removeTask(taskId);

  // Callback must have been fired with nullptr.
  EXPECT_TRUE(callbackFired);
  EXPECT_TRUE(receivedNullptr);
}

// Test BUG B scenario (b): Producer starts but crashes before noMoreData().
// initializeTask() was called but noMoreData() was never called.
// removeTask() must fire pending callbacks with nullptr.
TEST_F(UcxOutputQueueManagerTest, callbackFiredOnTerminateAfterInit) {
  const std::string taskId = "crashTest";

  auto task =
      initializeTask(taskId, 2 /* numDestinations */, 1 /* numDrivers */);

  // Register callbacks on both destinations.
  bool callback0Fired = false;
  bool callback0Nullptr = false;
  bool callback1Fired = false;
  bool callback1Nullptr = false;
  queueManager_->getData(
      taskId,
      0,
      [&callback0Fired, &callback0Nullptr](
          std::shared_ptr<UcxGpuPayload> data,
          std::vector<int64_t> remainingBytes) {
        callback0Fired = true;
        callback0Nullptr = (data == nullptr);
      });
  queueManager_->getData(
      taskId,
      1,
      [&callback1Fired, &callback1Nullptr](
          std::shared_ptr<UcxGpuPayload> data,
          std::vector<int64_t> remainingBytes) {
        callback1Fired = true;
        callback1Nullptr = (data == nullptr);
      });

  // Neither callback should fire yet.
  EXPECT_FALSE(callback0Fired);
  EXPECT_FALSE(callback1Fired);

  // Simulate task failure: abort the task so isRunning() returns false,
  // which is required by terminate()'s VELOX_CHECK.
  task->requestAbort().wait();

  queueManager_->removeTask(taskId);

  EXPECT_TRUE(callback0Fired);
  EXPECT_TRUE(callback0Nullptr);
  EXPECT_TRUE(callback1Fired);
  EXPECT_TRUE(callback1Nullptr);
}

// --- Broadcast tests ---

// Basic broadcast: enqueue data, all destinations receive the same data.
TEST_F(UcxOutputQueueManagerTest, broadcastBasic) {
  const vector_size_t size = 100;
  const std::string taskId = "broadcast0";
  const int numDestinations = 3;

  auto task = initializeTask(
      taskId,
      numDestinations,
      1 /* numDrivers */,
      core::PartitionedOutputNode::Kind::kBroadcast);

  // Broadcast must not be finished before noMoreQueues.
  EXPECT_FALSE(queueManager_->isFinished(taskId));

  // Tell the queue manager the final number of destinations.
  queueManager_->updateOutputBuffers(taskId, numDestinations, true);

  // Enqueue one batch (broadcast always uses destination 0).
  enqueue(taskId, 0, size);

  // All destinations should receive the data.
  for (int dest = 0; dest < numDestinations; ++dest) {
    fetch(taskId, dest);
  }

  // Signal no more data.
  noMoreData(taskId);

  // Fetch end markers from all destinations.
  for (int dest = 0; dest < numDestinations; ++dest) {
    fetchEndMarker(taskId, dest);
  }

  EXPECT_TRUE(queueManager_->isFinished(taskId));
  queueManager_->removeTask(taskId);
}

// Broadcast: late destination receives backfilled data.
TEST_F(UcxOutputQueueManagerTest, broadcastLateDestination) {
  const vector_size_t size = 50;
  const std::string taskId = "broadcast1";

  // Start with 2 destinations.
  auto task = initializeTask(
      taskId,
      2 /* numDestinations */,
      1 /* numDrivers */,
      core::PartitionedOutputNode::Kind::kBroadcast);

  // Enqueue 2 batches before the 3rd destination arrives.
  enqueue(taskId, 0, size);
  enqueue(taskId, 0, size);

  // Fetch from existing destinations to verify data is there.
  fetch(taskId, 0);
  fetch(taskId, 1);

  // Now add a 3rd destination (late arrival). It should be backfilled.
  queueManager_->updateOutputBuffers(taskId, 3, false);

  // The new destination should have both batches.
  fetch(taskId, 2);
  fetch(taskId, 2);

  // Finalize destinations and data.
  queueManager_->updateOutputBuffers(taskId, 3, true);
  noMoreData(taskId);

  // Fetch remaining data + end markers.
  fetch(taskId, 0);
  fetchEndMarker(taskId, 0);
  fetch(taskId, 1);
  fetchEndMarker(taskId, 1);
  fetchEndMarker(taskId, 2);

  EXPECT_TRUE(queueManager_->isFinished(taskId));
  queueManager_->removeTask(taskId);
}

// Broadcast: a destination can register before the coordinator publishes its
// buffer ID. Retained data must be available immediately, and the later
// buffer-count update must not duplicate it.
TEST_F(
    UcxOutputQueueManagerTest,
    broadcastDestinationRegistersBeforeBufferUpdate) {
  const vector_size_t size = 50;
  const std::string taskId = "broadcast-register-first";

  auto task = initializeTask(
      taskId,
      1 /* numDestinations */,
      1 /* numDrivers */,
      core::PartitionedOutputNode::Kind::kBroadcast);

  enqueue(taskId, 0, size);
  enqueue(taskId, 0, size);

  int receivedData = 0;
  for (int i = 0; i < 2; ++i) {
    queueManager_->getData(
        taskId,
        1,
        [&receivedData](
            std::shared_ptr<UcxGpuPayload> data,
            std::vector<int64_t> /*remainingBytes*/) {
          ASSERT_NE(data, nullptr);
          ++receivedData;
        });
    EXPECT_EQ(receivedData, i + 1);
  }

  queueManager_->updateOutputBuffers(taskId, 2, false);

  bool receivedEndMarker = false;
  queueManager_->getData(taskId, 1, receiveEndMarker(1, receivedEndMarker));
  EXPECT_FALSE(receivedEndMarker);

  // Publishing the already-created destination is idempotent and must not
  // manufacture another retained payload.
  queueManager_->updateOutputBuffers(taskId, 2, false);
  EXPECT_FALSE(receivedEndMarker);

  queueManager_->updateOutputBuffers(taskId, 2, true);
  EXPECT_ANY_THROW(queueManager_->getData(taskId, 2, [](auto, auto) {}));

  noMoreData(taskId);
  EXPECT_TRUE(receivedEndMarker);
  deleteResults(taskId, 1);

  fetch(taskId, 0);
  fetch(taskId, 0);
  fetchEndMarker(taskId, 0);

  EXPECT_TRUE(queueManager_->isFinished(taskId));
  queueManager_->removeTask(taskId);
}

// Broadcast: a destination registered after production finishes receives all
// retained data before exactly one end marker.
TEST_F(UcxOutputQueueManagerTest, broadcastLateRegistrationAfterEnd) {
  const std::string taskId = "broadcast-register-after-end";

  auto task = initializeTask(
      taskId,
      1 /* numDestinations */,
      1 /* numDrivers */,
      core::PartitionedOutputNode::Kind::kBroadcast);

  enqueue(taskId, 0, 10);
  noMoreData(taskId);

  fetch(taskId, 1);
  fetchEndMarker(taskId, 1);

  queueManager_->updateOutputBuffers(taskId, 2, true);
  fetch(taskId, 0);
  fetchEndMarker(taskId, 0);

  EXPECT_TRUE(queueManager_->isFinished(taskId));
  queueManager_->removeTask(taskId);
}

// Broadcast: isFinished requires noMoreQueues.
TEST_F(UcxOutputQueueManagerTest, broadcastNotFinishedWithoutNoMoreQueues) {
  const std::string taskId = "broadcast2";

  auto task = initializeTask(
      taskId,
      2 /* numDestinations */,
      1 /* numDrivers */,
      core::PartitionedOutputNode::Kind::kBroadcast);

  // Signal no more data.
  noMoreData(taskId);

  // Fetch end markers and delete results from all destinations.
  fetchEndMarker(taskId, 0);
  fetchEndMarker(taskId, 1);

  // Still not finished because noMoreQueues hasn't been signaled.
  EXPECT_FALSE(queueManager_->isFinished(taskId));

  // Signal no more queues.
  queueManager_->updateOutputBuffers(taskId, 2, true);

  EXPECT_TRUE(queueManager_->isFinished(taskId));
  queueManager_->removeTask(taskId);
}

// Broadcast: end marker propagated to late-arriving destinations.
TEST_F(UcxOutputQueueManagerTest, broadcastEndMarkerToLateDestination) {
  const std::string taskId = "broadcast3";

  auto task = initializeTask(
      taskId,
      1 /* numDestinations */,
      1 /* numDrivers */,
      core::PartitionedOutputNode::Kind::kBroadcast);

  enqueue(taskId, 0, 10);
  noMoreData(taskId);

  // Add a late destination after end marker was set.
  queueManager_->updateOutputBuffers(taskId, 2, true);

  // Destination 0: data + end marker.
  fetch(taskId, 0);
  fetchEndMarker(taskId, 0);

  // Destination 1 (late): backfilled data + end marker.
  fetch(taskId, 1);
  fetchEndMarker(taskId, 1);

  EXPECT_TRUE(queueManager_->isFinished(taskId));
  queueManager_->removeTask(taskId);
}
