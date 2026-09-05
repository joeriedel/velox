//
// NOTE: There may be a reason that we do need the transportKind
// to be baked into the presto/velox plan that I am not aware of,
// in which case none of what I did here makes sense, but it was
// fun to do anyway, and I am coming into this with only the
// understanding that we want to start getting all the work Kyle
// is doing rolled out into velox mainline.
//
// So with that goal in mind...
//
// My general goal when working to graduate experimental or research
// features into mainline in external projects is to try and reduce
// or eliminate changes to the project's core. This is in addition
// to obvious things like test/regression coverage etc.
//
// Changes to the core have a higher bar for acceptance and usually
// the "I need it for my feature" aren't generally convincing to
// maintainers.
//
// Of course cuDF and cuVS are incredible pieces of work with _real_
// value and so is Kyle's work here, so I make no argument that
// we can't get velox core changes merged where necessary, but the
// more we can avoid doing that the easier it will be.
//
// Kyle's research branch had made changes to the Prestissmo and Velox
// core in order to assign a transport type in the plan for the exchange.
//
// I wanted to see if it was reasonable to make the UCX topology self
// discovering, and not have to modfiy Presto or Velox cores at all.
//
// This is a proof of concept that require no Velox core changes
// and allows the exchange's to self discover the UCX availability.
//
//
//
//

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>

#include "velox/exec/Exchange.h"
#include "velox/exec/ExchangeSource.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/experimental/ucx-exchange/AutoExchangeSource.h"
#include "velox/experimental/ucx-exchange/Communicator.h"
#include "velox/experimental/ucx-exchange/OutputBufferReader.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/PrestoSerializer.h"

#ifdef VELOX_ENABLE_CUDF
#include <cudf/contiguous_split.hpp>
#include <cudf/table/table_view.hpp>
#include "velox/exec/DefaultOutputBufferManager.h"
#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/vector/CudfVector.h"
#include "velox/experimental/ucx-exchange/CudfPackedExchange.h"
#include "velox/experimental/ucx-exchange/CudfPackedOutput.h"
#include "velox/experimental/ucx-exchange/UcxOutputQueueManager.h"
#include "velox/experimental/ucx-exchange/UcxPartitionedOutput.h"
#endif
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::ucx_exchange;

namespace {

constexpr vector_size_t kNumRows{20'000};
constexpr vector_size_t kBatchSize{2'000};

int gFailures{0};

// Counts how many tasks the fallback served. The UCX factory claims every
// remote peer optimistically, so the fallback running at all is the signal
// that UCX did not answer -- which is what tells the two apart.
std::atomic<int> gFallbackServed{0};

void check(bool condition, std::string_view what) {
  std::cout << "  [" << (condition ? "PASS" : "FAIL") << "] " << what << "\n";
  if (!condition) {
    ++gFailures;
  }
}

// The fallback, wrapped so the run can tell when it was used. Passed to
// makeAutoUcxFactory as the transport for peers that do not answer, and
// registered on its own for producers whose task id is not a URL.
std::shared_ptr<ExchangeSource> countingFallbackFactory(
    const std::string& taskId,
    int destination,
    std::shared_ptr<ExchangeQueue> queue,
    memory::MemoryPool* pool) {
  auto source =
      createAutoExchangeSource(taskId, destination, std::move(queue), pool);
  if (source != nullptr) {
    gFallbackServed.fetch_add(1, std::memory_order_relaxed);
  }
  return source;
}

struct RunResult {
  int64_t rows{0};
  int64_t checksum{0};
  // The operator types that ran on each side.
  std::vector<std::string> producerOps;
  std::vector<std::string> consumerOps;
};

std::vector<std::string> operatorTypes(const std::shared_ptr<Task>& task) {
  std::vector<std::string> types;
  for (const auto& pipeline : task->taskStats().pipelineStats) {
    for (const auto& op : pipeline.operatorStats) {
      types.push_back(op.operatorType);
    }
  }
  return types;
}

bool contains(const std::vector<std::string>& types, std::string_view name) {
  return std::find(types.begin(), types.end(), name) != types.end();
}

RunResult runQuery(
    const std::string& producerId,
    const std::string& consumerId,
    memory::MemoryPool* pool,
    folly::Executor* executor) {
  facebook::velox::test::VectorMaker maker{pool};
  std::vector<RowVectorPtr> input;
  for (vector_size_t offset = 0; offset < kNumRows; offset += kBatchSize) {
    input.push_back(maker.rowVector(
        {"id"}, {maker.flatVector<int64_t>(kBatchSize, [offset](auto row) {
          return offset + row;
        })}));
  }

  // Plain PartitionedOutput writing to the stock output buffer. The plan
  // names no transport.
  auto producerPlan = PlanBuilder()
                          .values(input)
                          .partitionedOutput({}, /*numPartitions=*/1)
                          .planFragment();
  auto producer = Task::create(
      producerId,
      producerPlan,
      /*destination=*/0,
      core::QueryCtx::create(executor),
      Task::ExecutionMode::kParallel);
  producer->start(/*maxDrivers=*/1);
  auto producerOps = operatorTypes(producer);

  core::PlanNodeId exchangeId;
  auto consumerPlan = PlanBuilder()
                          .exchange(asRowType(input[0]->type()), "Presto")
                          .capturePlanNodeId(exchangeId)
                          .planFragment();

  std::atomic<int64_t> rows{0};
  std::atomic<int64_t> checksum{0};
  auto collect =
      [&rows, &checksum](
          RowVectorPtr data, bool /*drained*/, ContinueFuture* /*future*/) {
        if (data != nullptr) {
          auto* ids = data->childAt(0)->as<SimpleVector<int64_t>>();
          int64_t sum{0};
          for (vector_size_t i = 0; i < data->size(); ++i) {
            sum += ids->valueAt(i);
          }
          rows.fetch_add(data->size(), std::memory_order_relaxed);
          checksum.fetch_add(sum, std::memory_order_relaxed);
        }
        return BlockingReason::kNotBlocked;
      };

  auto consumer = Task::create(
      consumerId,
      consumerPlan,
      /*destination=*/0,
      core::QueryCtx::create(executor),
      Task::ExecutionMode::kParallel,
      collect);
  consumer->start(/*maxDrivers=*/1);
  auto consumerOps = operatorTypes(consumer);
  consumer->addSplit(
      exchangeId, Split(std::make_shared<RemoteConnectorSplit>(producerId)));
  consumer->noMoreSplits(exchangeId);

  consumer->taskCompletionFuture().wait(std::chrono::seconds(60));
  producer->taskCompletionFuture().wait(std::chrono::seconds(60));

  return RunResult{
      rows.load(),
      checksum.load(),
      std::move(producerOps),
      std::move(consumerOps)};
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};

  const uint16_t listenerPort = [] {
    if (const char* value = std::getenv("VELOX_UCX_LAB_PORT")) {
      return static_cast<uint16_t>(std::stoi(value));
    }
    return static_cast<uint16_t>(30460);
  }();

  memory::MemoryManager::initialize(memory::MemoryManager::Options{});

  // Connections a listener accepted stay in TIME_WAIT on its port for about a
  // minute after it closes. That is normal TCP, not a leak, but it stops the
  // port being rebound, so two runs back to back fail with ucxx::BusyError.
  // SO_REUSEADDR avoids that. UCX reads this variable when it builds the UCP
  // context, so it has to be set before the communicator is created.
  setenv("UCX_CM_REUSEADDR", "y", /*overwrite=*/0);

  if (!isRegisteredNamedVectorSerde(
          VectorSerde::kindName(VectorSerde::Kind::kPresto))) {
    serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }

  // PlanBuilder::filter() parses an expression, which needs both.
  functions::prestosql::registerAllScalarFunctions();
  parse::registerTypeResolver();

  std::cout << "== 0. one listener, both factories registered ==\n";
  setenv("VELOX_UCX_AUTO_PORT", std::to_string(listenerPort).c_str(), 1);
  ContinueFuture ready = ContinueFuture::makeEmpty();
  auto communicator = Communicator::initAndGet(listenerPort, "", &ready);
  check(communicator != nullptr, "communicator created");
  if (communicator == nullptr) {
    return 1;
  }
  ExchangeSource::factories().clear();
  registerAutoTransport(/*enableUcx=*/true, countingFallbackFactory);

  std::thread progressThread([communicator]() { communicator->run(); });
  if (ready.valid()) {
    std::move(ready).wait();
  }

  std::cout << "  factories: [UCX(fallback), fallback], listener on "
            << listenerPort << "\n";

  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(8);
  auto rootPool = memory::memoryManager()->addRootPool("selection_lab");
  auto pool = rootPool->addLeafChild("selection_lab_leaf");

  constexpr int64_t kExpectedSum{
      static_cast<int64_t>(kNumRows) * (kNumRows - 1) / 2};

  std::cout << "\n== A. a peer that speaks UCX, on its very first query ==\n";
  {
    const int fallbackBefore = gFallbackServed.load();
    auto result = runQuery(
        "http://127.0.0.1:8080/v1/task/producer-a",
        "http://127.0.0.1:8080/v1/task/consumer-a",
        pool.get(),
        executor.get());
    // Nothing told the process this peer speaks UCX, and no earlier query
    // warmed anything. The source asked the peer on its first request, and the
    // answer came back in time to carry that same request.
    check(
        gFallbackServed.load() == fallbackBefore,
        "UCX carried the first exchange, the fallback was never consulted");
    check(result.rows == kNumRows, "every row arrived over UCX");
    check(result.checksum == kExpectedSum, "values are correct");

    // The point of the exercise. UCX carried this query and neither side ran
    // an operator that knows what UCX is.
    std::cout << "  producer ops:";
    for (const auto& op : result.producerOps) {
      std::cout << " " << op;
    }
    std::cout << "\n  consumer ops:";
    for (const auto& op : result.consumerOps) {
      std::cout << " " << op;
    }
    std::cout << "\n";
    check(
        contains(result.producerOps, "PartitionedOutput"),
        "producer ran the STANDARD PartitionedOutput");
    check(
        !contains(result.producerOps, "UcxCpuRowPartitionedOutput") &&
            !contains(result.producerOps, "cudfPartitionedOutput"),
        "producer ran no UCX output operator");
    check(
        contains(result.consumerOps, "Exchange"),
        "consumer ran the PLAIN exec::Exchange");
    check(
        !contains(result.consumerOps, "UcxCpuRowExchange") &&
            !contains(result.consumerOps, "UcxExchange"),
        "consumer ran no UCX exchange operator");
  }

  std::cout << "\n== B. a peer that does not answer UCX ==\n";
  // Same plan. Only the peer changes, to a port with nothing listening.
  setenv("VELOX_UCX_AUTO_PORT", std::to_string(listenerPort + 77).c_str(), 1);
  std::cout << "  UCX now pointed at port " << (listenerPort + 77)
            << " (nothing listening)\n";
  {
    const int fallbackBefore = gFallbackServed.load();
    auto result = runQuery(
        "http://127.0.0.1:8080/v1/task/producer-b",
        "http://127.0.0.1:8080/v1/task/consumer-b",
        pool.get(),
        executor.get());
    check(
        gFallbackServed.load() == fallbackBefore + 1,
        "the fallback carried it instead");
    check(result.rows == kNumRows, "every row still arrived");
    check(result.checksum == kExpectedSum, "values are still correct");
  }

#ifdef VELOX_ENABLE_CUDF
  std::cout << "\n== C. a cuDF producer writing device pages to the stock "
               "buffer ==\n";
  // The GPU path, producer through consumer. The producer is a cuDF plan: it
  // packs its output into cudf::packed_columns and, with
  // VELOX_UCX_STOCK_OUTPUT_BUFFER set, enqueues that into the ordinary output
  // buffer as an IOBuf chain of [host metadata][device data], rather than into
  // a queue owned by a transport.
  //
  // The consumer plan names no transport. Its source is chosen by probing, and
  // CudfPackedExchange replaces exec::Exchange because the plan is a cuDF
  // plan. Those are two separate decisions: where the data lives picks the
  // operator, and whether the peer is reachable picks the transport.
  if (std::getenv("VELOX_UCX_STOCK_OUTPUT_BUFFER") == nullptr) {
    std::cout
        << "  skipped: set VELOX_UCX_STOCK_OUTPUT_BUFFER=1 to run it.\n"
        << "  That flag sends UcxPartitionedOutput's enqueue, flow control\n"
        << "  and end-of-stream to the stock output buffer instead of the UCX\n"
        << "  queue manager. It is off by default so the existing cuDF\n"
        << "  exchange tests run unchanged.\n";
  } else {
    cudf_velox::registerCudf();

    // Both halves of the GPU path are selected because the plan is a cuDF
    // plan. Neither reads a transport out of the plan, and no entry is put in
    // OutputTransportRegistry, so the task initializes the ordinary output
    // buffer and the packed pages go into it as any other pages would.
    registerCudfPackedOutput();
    registerCudfPackedExchange();

    facebook::velox::test::VectorMaker maker{pool.get()};
    auto gpuInput = maker.rowVector(
        {"id"},
        {maker.flatVector<int64_t>(kNumRows, [](auto row) { return row; })});

    // Starts a cuDF producer writing device pages into the stock buffer.
    auto startGpuProducer = [&](const std::string& taskId) {
      auto plan = PlanBuilder()
                      .values({gpuInput})
                      .partitionedOutput(/*keys=*/{}, /*numPartitions=*/1)
                      .planFragment();
      auto task = Task::create(
          taskId,
          plan,
          /*destination=*/0,
          core::QueryCtx::create(executor.get()),
          Task::ExecutionMode::kParallel);
      task->start(/*maxDrivers=*/1);
      return task;
    };

    struct GpuRun {
      int64_t rows{0};
      int64_t checksum{0};
      int cudfVectors{0};
      std::vector<std::string> producerOps;
      std::vector<std::string> consumerOps;
    };

    // Runs one producer into one consumer. 'filterExpr' empty means the
    // consumer is nothing but the exchange; otherwise a cuDF operator sits
    // downstream of it.
    auto runGpuQuery = [&](const std::string& taskId,
                           const std::string& filterExpr) {
      auto producer = startGpuProducer(taskId);

      core::PlanNodeId exchangeId;
      auto builder = PlanBuilder()
                         .exchange(asRowType(gpuInput->type()), "Presto")
                         .capturePlanNodeId(exchangeId);
      if (!filterExpr.empty()) {
        builder = builder.filter(filterExpr);
      }
      auto consumerPlan = builder.planFragment();

      GpuRun run;
      std::atomic<int64_t> rows{0};
      std::atomic<int64_t> checksum{0};
      std::atomic<int> cudfVectors{0};

      // Counts rows whether they arrive as device vectors or as host rows.
      // ToCudf puts a CudfToVelox in front of a sink it does not know, so a
      // consumer with a cuDF operator downstream delivers host rows even
      // though the exchange handed it device data.
      auto collect =
          [&](RowVectorPtr data, bool /*drained*/, ContinueFuture* /*future*/) {
            if (data == nullptr) {
              return BlockingReason::kNotBlocked;
            }
            int64_t sum{0};
            vector_size_t numRows{0};
            if (auto* cudfVector =
                    dynamic_cast<cudf_velox::CudfVector*>(data.get())) {
              cudfVectors.fetch_add(1, std::memory_order_relaxed);
              auto view = cudfVector->getTableView();
              numRows = view.num_rows();
              std::vector<int64_t> host(numRows, 0);
              cudaMemcpy(
                  host.data(),
                  view.column(0).data<int64_t>(),
                  numRows * sizeof(int64_t),
                  cudaMemcpyDeviceToHost);
              for (auto value : host) {
                sum += value;
              }
            } else {
              numRows = data->size();
              auto* ids = data->childAt(0)->as<SimpleVector<int64_t>>();
              for (vector_size_t i = 0; i < numRows; ++i) {
                sum += ids->valueAt(i);
              }
            }
            rows.fetch_add(numRows, std::memory_order_relaxed);
            checksum.fetch_add(sum, std::memory_order_relaxed);
            return BlockingReason::kNotBlocked;
          };

      auto consumer = Task::create(
          taskId + "-consumer",
          consumerPlan,
          /*destination=*/0,
          core::QueryCtx::create(executor.get()),
          Task::ExecutionMode::kParallel,
          collect);
      consumer->start(/*maxDrivers=*/1);

      run.producerOps = operatorTypes(producer);
      run.consumerOps = operatorTypes(consumer);

      consumer->addSplit(
          exchangeId, Split(std::make_shared<RemoteConnectorSplit>(taskId)));
      consumer->noMoreSplits(exchangeId);
      consumer->taskCompletionFuture().wait(std::chrono::seconds(60));

      producer->requestCancel();
      producer->taskCompletionFuture().wait(std::chrono::seconds(30));

      run.rows = rows.load();
      run.checksum = checksum.load();
      run.cudfVectors = cudfVectors.load();
      return run;
    };

    auto printOps = [](const char* label, const std::vector<std::string>& ops) {
      std::cout << "  " << label << ":";
      for (const auto& op : ops) {
        std::cout << " " << op;
      }
      std::cout << "\n";
    };

    // D1: nothing downstream of the exchange but the sink.
    //
    // The checksum is what proves the pages were really on the device.
    // CudfPackedExchange reads them with cudf::unpack, which takes the second
    // chain segment as a device pointer; had the producer written host bytes
    // there, the result would be garbage rather than the exact input sum. A
    // plain exec::Exchange on the same pages fails outright -- it reports
    // "Received corrupted serialized page" when the Presto serde reads them.
    {
      auto run = runGpuQuery("gpu-stock-producer", /*filterExpr=*/"");
      printOps("producer ops", run.producerOps);
      printOps("consumer ops", run.consumerOps);
      std::cout << "  rows " << run.rows << ", checksum " << run.checksum
                << "\n";
      // The plan named no transport and nothing was put in
      // OutputTransportRegistry: both operators were chosen because the plan
      // is a cuDF plan.
      check(
          contains(run.producerOps, "cudfPartitionedOutput"),
          "producer ran the cuDF packing output, with no transport in the plan");
      check(
          contains(run.consumerOps, "CudfPackedExchange"),
          "consumer ran CudfPackedExchange, not exec::Exchange");
      // ToCudf puts the device-to-host boundary after the exchange, which is
      // only correct if the exchange hands it device data.
      check(
          contains(run.consumerOps, "CudfToVelox"),
          "the exchange's output was on the device");
      check(
          !contains(run.consumerOps, "CudfFromVelox"),
          "nothing was uploaded in front of it");
      check(run.rows == kNumRows, "every GPU row crossed the exchange");
      check(run.checksum == kExpectedSum, "GPU values are correct");
    }

    // D2: a real cuDF operator downstream. ToCudf compiles the filter, which
    // means its DriverAdapter returns true -- so this only works because the
    // exchange is substituted by an adapter that runs first and returns false,
    // and because an OperatorAdapter tells ToCudf the exchange already
    // produces device data.
    std::cout << "\n== D. the same exchange feeding a real cuDF operator ==\n";
    {
      constexpr int64_t kCutoff{10'000};
      constexpr int64_t kFilteredSum{kCutoff * (kCutoff - 1) / 2};
      auto run =
          runGpuQuery("gpu-filtered-producer", fmt::format("id < {}", kCutoff));
      printOps("consumer ops", run.consumerOps);
      std::cout << "  rows " << run.rows << ", checksum " << run.checksum
                << "\n";
      check(
          contains(run.consumerOps, "CudfPackedExchange"),
          "the exchange survived ToCudf's compile pass");
      check(
          contains(run.consumerOps, "CudfFilterProject"),
          "ToCudf compiled the filter onto the GPU");
      check(
          !contains(run.consumerOps, "CudfFromVelox"),
          "ToCudf did not insert an upload in front of device data");
      check(run.rows == kCutoff, "the filter kept the right number of rows");
      check(run.checksum == kFilteredSum, "filtered values are correct");
    }

    // E: the same producer read by someone who never said they take device
    // memory. Drained straight through OutputBufferReader, so nothing records
    // the destination in DevicePageReaders and the page has to render itself
    // as ordinary host bytes.
    std::cout << "\n== E. the same pages, read by a host-only reader ==\n";
    {
      auto hostProducer = startGpuProducer("gpu-host-reader");
      auto manager = exec::DefaultOutputBufferManager::getInstanceRef();
      auto reader = std::make_shared<OutputBufferReader>(
          manager,
          "gpu-host-reader",
          /*destination=*/0,
          /*maxBytes=*/1 << 20,
          /*startSequence=*/0);

      std::vector<std::unique_ptr<folly::IOBuf>> collected;
      bool atEnd{false};
      for (int attempt = 0; attempt < 200 && !atEnd; ++attempt) {
        std::atomic<bool> done{false};
        reader->request(1 << 20, [&](OutputBufferReader::Frame frame) {
          for (auto& page : frame.pages) {
            collected.push_back(std::move(page));
          }
          atEnd = frame.atEnd;
          done.store(true);
        });
        while (!done.load()) {
          std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
      }

      int64_t hostRows{0};
      int64_t hostChecksum{0};
      bool allHostChains{true};
      auto* serde = getNamedVectorSerde(
          VectorSerde::kindName(VectorSerde::Kind::kPresto));
      for (auto& iobuf : collected) {
        // Ask CUDA where each segment lives. A device rendering puts the
        // column data in device memory; a host rendering is entirely host
        // memory, and that is the difference being checked. Chain length says
        // nothing -- a serialized host page is a chain of several buffers.
        const auto* segment = iobuf.get();
        do {
          cudaPointerAttributes attributes{};
          const auto status =
              cudaPointerGetAttributes(&attributes, segment->data());
          if (status != cudaSuccess) {
            // Plain host memory CUDA has never seen.
            cudaGetLastError();
          } else if (attributes.type == cudaMemoryTypeDevice) {
            allHostChains = false;
          }
          segment = segment->next();
        } while (segment != iobuf.get());

        auto page =
            std::make_unique<exec::PrestoSerializedPage>(std::move(iobuf));
        auto stream = page->prepareStreamForDeserialize();
        RowVectorPtr result;
        while (!stream->atEnd()) {
          VectorStreamGroup::read(
              stream.get(),
              pool.get(),
              asRowType(gpuInput->type()),
              serde,
              &result,
              nullptr);
          auto* ids = result->childAt(0)->as<SimpleVector<int64_t>>();
          for (vector_size_t i = 0; i < result->size(); ++i) {
            hostChecksum += ids->valueAt(i);
          }
          hostRows += result->size();
        }
      }

      std::cout << "  pages " << collected.size() << ", rows " << hostRows
                << ", checksum " << hostChecksum << "\n";
      check(allHostChains, "pages rendered as host bytes, not a device chain");
      check(hostRows == kNumRows, "every row came back through the host path");
      check(hostChecksum == kExpectedSum, "host-rendered values are correct");

      reader->close();
      reader->deleteResults();
      hostProducer->requestCancel();
      hostProducer->taskCompletionFuture().wait(std::chrono::seconds(30));
    }

    // F: a cuDF consumer reading a producer that is not a cuDF plan at all.
    // cudf.enabled is off for the producer's query, so ToCudf and the packing
    // adapter both skip it and it sends ordinary Presto pages. The consumer is
    // still a cuDF plan, so CudfPackedExchange is substituted -- and has to
    // cope with what actually arrived rather than what it expected.
    //
    // This is the CPU-fallback shape, and allowCpuFallback defaults to true,
    // so it is the ordinary case rather than a corner.
    std::cout << "\n== F. a cuDF consumer reading a plain Presto producer ==\n";
    {
      auto plainPlan = PlanBuilder()
                           .values({gpuInput})
                           .partitionedOutput(/*keys=*/{}, /*numPartitions=*/1)
                           .planFragment();
      const std::string plainTaskId{"plain-presto-producer"};
      auto plainProducer = Task::create(
          plainTaskId,
          plainPlan,
          /*destination=*/0,
          core::QueryCtx::create(
              executor.get(),
              core::QueryConfig{
                  {{std::string(cudf_velox::CudfConfig::kCudfEnabled),
                    "false"}}}),
          Task::ExecutionMode::kParallel);
      plainProducer->start(/*maxDrivers=*/1);
      auto plainOps = operatorTypes(plainProducer);

      core::PlanNodeId mixedExchangeId;
      auto mixedPlan = PlanBuilder()
                           .exchange(asRowType(gpuInput->type()), "Presto")
                           .capturePlanNodeId(mixedExchangeId)
                           .planFragment();

      std::atomic<int64_t> mixedRows{0};
      std::atomic<int64_t> mixedChecksum{0};
      auto collectMixed = [&](RowVectorPtr data, bool, ContinueFuture*) {
        if (data != nullptr) {
          auto* ids = data->childAt(0)->as<SimpleVector<int64_t>>();
          for (vector_size_t i = 0; i < data->size(); ++i) {
            mixedChecksum.fetch_add(ids->valueAt(i), std::memory_order_relaxed);
          }
          mixedRows.fetch_add(data->size(), std::memory_order_relaxed);
        }
        return BlockingReason::kNotBlocked;
      };

      auto mixedConsumer = Task::create(
          "mixed-consumer",
          mixedPlan,
          /*destination=*/0,
          core::QueryCtx::create(executor.get()),
          Task::ExecutionMode::kParallel,
          collectMixed);
      mixedConsumer->start(/*maxDrivers=*/1);
      auto mixedOps = operatorTypes(mixedConsumer);
      mixedConsumer->addSplit(
          mixedExchangeId,
          Split(std::make_shared<RemoteConnectorSplit>(plainTaskId)));
      mixedConsumer->noMoreSplits(mixedExchangeId);
      mixedConsumer->taskCompletionFuture().wait(std::chrono::seconds(60));

      printOps("producer ops", plainOps);
      printOps("consumer ops", mixedOps);
      std::cout << "  rows " << mixedRows.load() << ", checksum "
                << mixedChecksum.load() << "\n";
      check(
          contains(plainOps, "PartitionedOutput"),
          "producer ran the STANDARD PartitionedOutput, not the packing one");
      check(
          contains(mixedOps, "CudfPackedExchange"),
          "consumer still ran CudfPackedExchange");
      check(
          mixedRows.load() == kNumRows,
          "the exchange coped with pages it did not pack");
      check(mixedChecksum.load() == kExpectedSum, "values are correct");

      plainProducer->requestCancel();
      plainProducer->taskCompletionFuture().wait(std::chrono::seconds(30));
    }
  }
#else
  std::cout << "\n== C. cuDF producer -- skipped, built without cuDF ==\n";
#endif

  communicator->stop();
  progressThread.join();

  std::cout << "\n"
            << (gFailures == 0 ? "ALL CHECKS PASSED" : "FAILURES PRESENT")
            << " (" << gFailures << " failure(s))" << std::endl;
  return gFailures == 0 ? 0 : 1;
}
