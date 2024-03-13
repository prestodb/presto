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

#include "velox/experimental/wave/common/tests/CudaTest.h"

#include <cuda_runtime.h> // @manual
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include "velox/buffer/Buffer.h"
#include "velox/common/base/AsyncSource.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/SelectivityInfo.h"
#include "velox/common/base/Semaphore.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/common/time/Timer.h"
#include "velox/experimental/wave/common/GpuArena.h"
#include "velox/experimental/wave/common/tests/BlockTest.h"

#include <iostream>

DEFINE_int32(num_streams, 0, "Number of paralll streams");
DEFINE_int32(op_size, 0, "Size of invoke kernel (ints read and written)");
DEFINE_int32(
    num_ops,
    0,
    "Number of consecutive kernel executions on each stream");
DEFINE_bool(
    use_callbacks,
    false,
    "Queue a host callback after each kernel execution");
DEFINE_bool(
    sync_streams,
    false,
    "Use events to synchronize all parallel streams before calling the next kernel on each stream.");
DEFINE_bool(
    prefetch,
    true,
    "Use prefetch to move unified memory to device at start and to host at end");
DEFINE_int32(num_threads, 1, "Threads in reduce test");
DEFINE_int64(working_size, 100000000, "Bytes in flight per thread");
DEFINE_int32(num_columns, 10, "Columns in reduce test");
DEFINE_int32(num_rows, 100000, "Batch size in reduce test");
DEFINE_int32(num_batches, 100, "Batches in reduce test");

DEFINE_int64(
    memcpy_bytes_per_thread,
    1000000,
    "Unit size of memcpy per thread");

DEFINE_string(mode, "all", "Mode for reduce test memory transfers.");

DEFINE_bool(enable_bm, false, "Enable custom and long running tests");

DEFINE_string(
    roundtrip_ops,
    "",
    "Custom roundtrip composition, see comments in RoundtripThread");
using namespace facebook::velox;
using namespace facebook::velox::wave;

// Dataset for data transfer test.
struct DataBatch {
  std::vector<BufferPtr> columns;
  // Sum of the int64s in buffers. Numbers below the size() of each are added
  // up.
  int64_t sum{0};
  int64_t byteSize;
  int64_t dataSize;
};

std::unique_ptr<folly::CPUThreadPoolExecutor> globalSyncExecutor;

folly::CPUThreadPoolExecutor* syncExecutor() {
  return globalSyncExecutor.get();
}

constexpr int32_t kBlockSize = 256;

struct ArenaSet {
  std::unique_ptr<GpuArena> unified;
  std::unique_ptr<GpuArena> device;
  std::unique_ptr<GpuArena> host;
};

struct RunStats {
  std::string mode;
  int32_t runId{-1};
  int32_t batchMB{0};
  int32_t numColumns{0};
  int32_t numRows{0};
  int32_t numThreads{0};
  int32_t workPerThread{0};
  float gbs{0};
  float resultClocks{0};
  int32_t copyPerThread{0};

  std::string toString() const {
    return fmt::format(
        "{} GB/s {} {}x{} rows ({}MB)  {} on thread, {} threads copy={}",
        gbs,
        mode,
        numColumns,
        numRows,
        (numColumns * numRows * sizeof(int64_t)) >> 20,
        workPerThread,
        numThreads,
        copyPerThread);
  }
};

/// Base class modeling processing a batch of data. Inits, continues and tests
/// for ready.
class ProcessBatchBase {
 public:
  virtual ~ProcessBatchBase() = default;
  // Starts processing 'batch'. Use isReady() to check for result.
  virtual void init(
      DataBatch* data,
      GpuArena* unifiedArena,
      GpuArena* deviceArena,
      GpuArena* hostArena,
      folly::CPUThreadPoolExecutor* executor) {
    data_ = data;
    unifiedArena_ = unifiedArena;
    deviceArena_ = deviceArena;
    hostArena_ = hostArena;
    executor_ = executor;
    numRows_ = data_->columns[0]->size() / sizeof(int64_t);
    numBlocks_ = bits::roundUp(numRows_, 256) / 256;
  }

  DataBatch* batch() {
    return data_;
  }
  // Returns true if ready and sets 'result'. Returns false if pending. If
  // 'wait' is true, blocks until ready.
  virtual bool isReady(int64_t& result, bool wait) = 0;

  auto resultClocks() const {
    return resultClocks_;
  }

 protected:
  Device* device_{getDevice()};
  DataBatch* data_{nullptr};
  int32_t numBlocks_;
  int32_t numRows_;

  GpuArena* unifiedArena_{nullptr};
  GpuArena* deviceArena_{nullptr};
  GpuArena* hostArena_{nullptr};
  folly::CPUThreadPoolExecutor* executor_{nullptr};
  std::vector<WaveBufferPtr> deviceBuffers_;
  std::vector<int64_t*> deviceArrays_;
  WaveBufferPtr result_;
  WaveBufferPtr hostResult_;
  int64_t sum_{0};
  std::vector<std::unique_ptr<BlockTestStream>> streams_;
  std::vector<std::unique_ptr<Event>> events_;
  Semaphore sem_{0};
  int32_t toAcquire_{0};
  float resultClocks_{0};
};

class ProcessUnifiedN : public ProcessBatchBase {
 public:
  void init(
      DataBatch* data,
      GpuArena* unifiedArena,
      GpuArena* deviceArena,
      GpuArena* hostArena,
      folly::CPUThreadPoolExecutor* executor) override {
    ProcessBatchBase::init(
        data, unifiedArena, deviceArena, hostArena, executor);
    result_ =
        unifiedArena->allocate<int64_t>(data_->columns.size() * numBlocks_);
    deviceBuffers_.resize(data->columns.size());
    streams_.resize(deviceBuffers_.size());
    events_.resize(deviceBuffers_.size());
    toAcquire_ = data->columns.size();
    for (auto i = 0; i < data_->columns.size(); ++i) {
      deviceBuffers_[i] =
          unifiedArena->allocate<char>(data_->columns[i]->size());
      executor_->add([i, this]() {
        setDevice(device_);
        /*simd::*/ memcpy(
            deviceBuffers_[i]->as<char>(),
            data_->columns[i]->as<char>(),
            data_->columns[i]->size());
        streams_[i] = std::make_unique<BlockTestStream>();
        streams_[i]->prefetch(
            device_, deviceBuffers_[i]->as<char>(), data_->columns[i]->size());
        auto resultIndex = i * numBlocks_;
        streams_[i]->testSum64(
            numBlocks_,
            deviceBuffers_[i]->as<int64_t>(),
            result_->as<int64_t>() + resultIndex);
        events_[i] = std::make_unique<Event>();
        events_[i]->record(*streams_[i]);
        sem_.release();
      });
    }
  }

  bool isReady(int64_t& result, bool wait) override {
    if (toAcquire_) {
      if (wait) {
        while (toAcquire_) {
          sem_.acquire();
          --toAcquire_;
        }
      } else {
        while (toAcquire_) {
          if (sem_.count() == 0) {
            return false;
          }
          sem_.acquire();
          --toAcquire_;
        }
      }
    }
    for (auto i = 0; i < events_.size(); ++i) {
      if (wait) {
        events_[i]->wait();
      } else {
        if (!events_[i]->query()) {
          return false;
        }
      }
    }
    int64_t sum = 0;
    auto resultPtr = result_->as<int64_t>();
    auto numResults = data_->columns.size() * numBlocks_;
    SelectivityInfo info;
    {
      SelectivityTimer s(info, 1);
      for (auto i = 0; i < numResults; ++i) {
        sum += resultPtr[i];
      }
    }
    resultClocks_ = info.timeToDropValue();
    result = sum;
    return true;
  }
};

class ProcessUnifiedCudaCopy : public ProcessBatchBase {
 public:
  void init(
      DataBatch* data,
      GpuArena* unifiedArena,
      GpuArena* deviceArena,
      GpuArena* hostArena,
      folly::CPUThreadPoolExecutor* executor) override {
    ProcessBatchBase::init(
        data, unifiedArena, deviceArena, hostArena, executor);
    result_ =
        unifiedArena->allocate<int64_t>(data_->columns.size() * numBlocks_);
    deviceBuffers_.resize(data->columns.size());
    streams_.resize(deviceBuffers_.size());
    events_.resize(deviceBuffers_.size());
    for (auto i = 0; i < data_->columns.size(); ++i) {
      deviceBuffers_[i] =
          unifiedArena->allocate<char>(data_->columns[i]->size());

      streams_[i] = std::make_unique<BlockTestStream>();
      streams_[i]->hostToDeviceAsync(
          deviceBuffers_[i]->as<char>(),
          data->columns[i]->as<char>(),
          data_->columns[i]->size());
      auto resultIndex = i * numBlocks_;
      streams_[i]->testSum64(
          numBlocks_,
          deviceBuffers_[i]->as<int64_t>(),
          result_->as<int64_t>() + resultIndex);
      events_[i] = std::make_unique<Event>();
      events_[i]->record(*streams_[i]);
    }
  }

  bool isReady(int64_t& result, bool wait) override {
    for (auto i = 0; i < events_.size(); ++i) {
      if (wait) {
        events_[i]->wait();
      } else {
        if (!events_[i]->query()) {
          return false;
        }
      }
    }
    int64_t sum = 0;
    auto resultPtr = result_->as<int64_t>();
    auto numResults = data_->columns.size() * numBlocks_;
    for (auto i = 0; i < numResults; ++i) {
      sum += resultPtr[i];
    }
    result = sum;
    return true;
  }
};

class ProcessDeviceCoalesced : public ProcessBatchBase {
 public:
  void init(
      DataBatch* data,
      GpuArena* unifiedArena,
      GpuArena* deviceArena,
      GpuArena* hostArena,
      folly::CPUThreadPoolExecutor* executor) override {
    ProcessBatchBase::init(
        data, unifiedArena, deviceArena, hostArena, executor);
    result_ =
        deviceArena->allocate<int64_t>(data_->columns.size() * numBlocks_);
    hostResult_ =
        hostArena->allocate<int64_t>(data_->columns.size() * numBlocks_);
    streams_.resize(1);
    streams_[0] = std::make_unique<BlockTestStream>();

    events_.resize(1);
    int64_t total = 0;
    for (auto i = 0; i < data_->columns.size(); ++i) {
      total += data->columns[i]->size();
    }

    transfer_ = hostArena->allocate<char>(total);
    compute_ = deviceArena->allocate<char>(total);
    auto destination = transfer_->as<char>();
    int32_t firstToCopy = 0;
    int64_t copySize = 0;
    auto targetCopySize = FLAGS_memcpy_bytes_per_thread;
    int32_t numThreads = 0;
    for (auto i = 0; i < data_->columns.size(); ++i) {
      auto columnSize = data->columns[i]->size();
      copySize += columnSize;
      if (copySize >= targetCopySize && i < data_->columns.size() - 1) {
        ++numThreads;
        executor->add([i, firstToCopy, destination, this]() {
          copyColumns(firstToCopy, i + 1, destination, true);
        });
        destination += copySize;
        copySize = 0;
        firstToCopy = i + 1;
      }
    }
    toAcquire_ = 1;
    syncExecutor()->add([firstToCopy, numThreads, destination, total, this]() {
      copyColumns(firstToCopy, data_->columns.size(), destination, false);
      for (auto i = 0; i < numThreads; ++i) {
        sem_.acquire();
      }
      streams_[0]->hostToDeviceAsync(
          compute_->as<char>(), transfer_->as<char>(), total);
      streams_[0]->testSum64(
          numBlocks_ * data_->columns.size(),
          compute_->as<int64_t>(),
          result_->as<int64_t>());
      streams_[0]->deviceToHostAsync(
          hostResult_->as<int64_t>(),
          result_->as<int64_t>(),
          data_->columns.size() * numBlocks_ * sizeof(int64_t));
      events_[0] = std::make_unique<Event>();
      events_[0]->record(*streams_[0]);
      syncSem_.release();
    });
  }

  bool isReady(int64_t& result, bool wait) override {
    if (toAcquire_) {
      if (wait) {
        syncSem_.acquire();
        --toAcquire_;
      } else {
        if (syncSem_.count() == 0) {
          return false;
        }
        syncSem_.acquire();
        --toAcquire_;
      }
    }
    if (wait) {
      events_[0]->wait();
    } else {
      if (!events_[0]->query()) {
        return false;
      }
    }
    int64_t sum = 0;
    auto resultPtr = hostResult_->as<int64_t>();
    auto numResults = data_->columns.size() * numBlocks_;
    for (auto i = 0; i < numResults; ++i) {
      sum += resultPtr[i];
    }
    result = sum;
    return true;
  }

 private:
  void
  copyColumns(int32_t begin, int32_t end, char* destination, bool release) {
    for (auto i = begin; i < end; ++i) {
      memcpy(
          destination,
          data_->columns[i]->as<char>(),
          data_->columns[i]->size());
      destination += data_->columns[i]->size();
    }
    if (release) {
      sem_.release();
    }
  }

  WaveBufferPtr transfer_;
  WaveBufferPtr compute_;
  WaveBufferPtr hostResult_;
  Semaphore syncSem_{0};
};

struct RoundtripStats {
  // id of experiment.
  int32_t id{0};
  bool isCpu{false};
  // Description of round trip.
  std::string mode;
  // Threads in experiment.
  int32_t numThreads{0};
  // Number of times a thread repeats the sequence given by 'mode'.
  int64_t numOps{0};
  // Number of additions in the ops
  int64_t numAdds{0};

  // Bytes transferred to device.
  int64_t toDeviceBytes{0};

  // Bytes copied to host.
  int64_t toHostBytes{0};

  int64_t startMicros{0};

  int64_t endMicros{0};

  // Wall time of experiment.
  float micros{0};
  void init(
      int32_t _id,
      bool _isCpu,
      int32_t _numThreads,
      std::string _mode,
      int32_t repeats) {
    id = _id;
    isCpu = _isCpu;
    numThreads = _numThreads;
    mode = _mode;
    numOps = repeats;
  }

  void add(RoundtripStats& other) {
    startMicros = std::min(startMicros, other.startMicros);
    endMicros = std::max(endMicros, other.endMicros);
    toHostBytes += other.toHostBytes;
    toDeviceBytes += other.toDeviceBytes;
    numAdds += other.numAdds;
  }

  std::string toString() const {
    return fmt::format(
        "{}: rps={} gips={}  mode={} threads={} micros={} avgus={} toDev={} GB/s toHost={} GB/s",
        id,
        (numThreads * numOps) / (micros / 1000000),
        numAdds / (micros * 1000),
        mode,
        numThreads,
        micros,
        micros / numOps,
        toDeviceBytes / (micros * 1000),
        toHostBytes / (micros * 1000));
  }
};

/// Describes one thread of execution in round trip measurement. Each thread
/// does a sequence of data transfers, kernel calls and synchronizations. The
/// operations are described in a string of the form:
///
///  dnnn - Transfer nnn KB to device.
/// hnnn - transfer nnn KB to host.
/// annn,xxx - Increment nnn KB of ints in a kernel xxx times. Reads and writes
/// nnn KB sequentially over up to 10K threads in 256 thread blocks rnnn,xxx -
/// Increment nnn KB of int32 counters by a random increment fetched from a
/// lookup table of nnn KB. This is done xxx times. Read and write sequential,
/// read random in up to 10K lanes in blocks of 256. wnnn,xxx Same as 'a' but
/// invokes the kernel wit a 8KB struct. Measures difference of sending a small
/// parameter block as kernel parameter as opposed to pre-staging it on device
/// with a small transfer. s - Synchronize the stream. e - Synchronize the
/// stream with record event + wait event.
class RoundtripThread {
 public:
  // Up to 32 MB of ints.
  static constexpr int32_t kNumKB = 32 << 10;
  static constexpr int32_t kNumInts = kNumKB * 256;

  RoundtripThread(int32_t device, ArenaSet* arenas) : arenas_(arenas) {
    setDevice(getDevice(device));
    hostBuffer_ = arenas_->host->allocate<int32_t>(kNumInts);
    deviceBuffer_ = arenas_->device->allocate<int32_t>(kNumInts);
    lookupBuffer_ = arenas_->device->allocate<int32_t>(kNumInts);

    stream_ = std::make_unique<TestStream>();
    event_ = std::make_unique<Event>();
    for (auto i = 0; i < kNumInts; ++i) {
      hostBuffer_->as<int32_t>()[i] = i;
    }
    stream_->hostToDeviceAsync(
        lookupBuffer_->as<int32_t>(),
        hostBuffer_->as<int32_t>(),
        kNumInts * sizeof(int32_t));
    stream_->wait();
    hostInts_ = std::make_unique<int32_t[]>(kNumInts);
    hostLookup_ = std::make_unique<int32_t[]>(kNumInts);
    memcpy(
        hostLookup_.get(),
        hostBuffer_->as<int32_t>(),
        kNumInts * sizeof(int32_t));
  }

  enum class OpCode {
    kToDevice,
    kToHost,
    kAdd,
    kAddRandom,
    kWideAdd,
    kEnd,
    kSync,
    kSyncEvent
  };

  struct Op {
    OpCode opCode;
    int32_t param1{1};
    int32_t param2{0};
  };

  void run(RoundtripStats& stats) {
    stats.startMicros = getCurrentTimeMicro();
    for (auto counter = 0; counter < stats.numOps; ++counter) {
      int32_t position = 0;
      bool done = false;
      for (;;) {
        auto op = nextOp(stats.mode, position);
        switch (op.opCode) {
          case OpCode::kEnd:
            done = true;
            break;
          case OpCode::kToDevice:
            VELOX_CHECK_LE(op.param1, kNumKB);
            if (stats.isCpu) {
              memcpy(
                  hostInts_.get(),
                  hostBuffer_->as<int32_t>(),
                  op.param1 * 1024);
            } else {
              stream_->hostToDeviceAsync(
                  deviceBuffer_->as<int32_t>(),
                  hostBuffer_->as<int32_t>(),
                  op.param1 * 1024);
            }
            stats.toDeviceBytes += op.param1 * 1024;
            break;
          case OpCode::kToHost:
            VELOX_CHECK_LE(op.param1, kNumKB);
            if (stats.isCpu) {
              memcpy(
                  hostBuffer_->as<int32_t>(),
                  hostInts_.get(),
                  op.param1 * 1024);
            } else {
              stream_->deviceToHostAsync(
                  hostBuffer_->as<int32_t>(),
                  deviceBuffer_->as<int32_t>(),
                  op.param1 * 1024);
            }
            stats.toHostBytes += op.param1 * 1024;
            break;
          case OpCode::kAdd:
            VELOX_CHECK_LE(op.param1, kNumKB);
            if (stats.isCpu) {
              addOneCpu(op.param1 * 256, op.param2);
            } else {
              stream_->addOne(
                  deviceBuffer_->as<int32_t>(), op.param1 * 256, op.param2);
            }
            stats.numAdds += op.param1 * op.param2 * 256;
            break;
          case OpCode::kWideAdd:
            VELOX_CHECK_LE(op.param1, kNumKB);
            if (stats.isCpu) {
              addOneCpu(op.param1 * 256, op.param2);
            } else {
              stream_->addOneWide(
                  deviceBuffer_->as<int32_t>(), op.param1 * 256, op.param2);
            }
            stats.numAdds += op.param1 * op.param2 * 256;
            break;

          case OpCode::kAddRandom:
            VELOX_CHECK_LE(op.param1, kNumKB);
            if (stats.isCpu) {
              addOneRandomCpu(op.param1 * 256, op.param2);
            } else {
              stream_->addOneRandom(
                  deviceBuffer_->as<int32_t>(),
                  lookupBuffer_->as<int32_t>(),
                  op.param1 * 256,
                  op.param2);
            }
            stats.numAdds += op.param1 * op.param2 * 256;
            break;

          case OpCode::kSync:
            if (!stats.isCpu) {
              stream_->wait();
            }
            break;
          case OpCode::kSyncEvent:
            if (!stats.isCpu) {
              event_->record(*stream_);
              event_->wait();
            }
            break;
          default:
            VELOX_FAIL("Bad test opcode {}", static_cast<int32_t>(op.opCode));
        }
        if (done) {
          break;
        }
      }
    }
    stats.endMicros = getCurrentTimeMicro();
  }

  void addOneCpu(int32_t size, int32_t repeat) {
    int32_t* ints = hostInts_.get();
    for (auto counter = 0; counter < repeat; ++counter) {
      for (auto i = 0; i < size; ++i) {
        ++ints[i];
      }
    }
  }
  void addOneRandomCpu(uint32_t size, int32_t repeat) {
    int32_t* ints = hostInts_.get();
    int32_t* lookup = hostLookup_.get();
    for (uint32_t counter = 0; counter < repeat; ++counter) {
      for (auto i = 0; i < size; ++i) {
        auto rnd = (static_cast<uint64_t>(
                        static_cast<uint32_t>(i * (counter + 1) * 1367836089)) *
                    size) >>
            32;
        ints[i] += lookup[rnd];
      }
    }
  }

  Op nextOp(const std::string& str, int32_t& position) {
    Op op;
    for (;;) {
      if (position >= str.size()) {
        op.opCode = OpCode::kEnd;
        return op;
      }
      switch (str[position]) {
        case ' ':
          ++position;
          break;
        case 'd':
          op.opCode = OpCode::kToDevice;
          ++position;
          op.param1 = parseInt(str, position, 1);
          return op;
        case 'h':
          op.opCode = OpCode::kToHost;
          ++position;
          op.param1 = parseInt(str, position, 1);
          return op;
        case 'a':
          op.opCode = OpCode::kAdd;
          ++position;
          op.param1 = parseInt(str, position, 1);
          op.param2 = parseInt(str, position, 1);
          return op;
        case 'w':
          op.opCode = OpCode::kWideAdd;
          ++position;
          op.param1 = parseInt(str, position, 1);
          op.param2 = parseInt(str, position, 1);
          return op;

        case 'r':
          op.opCode = OpCode::kAddRandom;
          ++position;
          op.param1 = parseInt(str, position, 1);
          op.param2 = parseInt(str, position, 1);
          return op;

        case 's':
          op.opCode = OpCode::kSync;
          ++position;
          return op;
        case 'e':
          op.opCode = OpCode::kSyncEvent;
          ++position;
          return op;
        default:
          VELOX_FAIL("No opcode {}", str[position]);
      }
    }
  }

  int32_t parseInt(const std::string& str, int32_t& position, int32_t deflt) {
    int32_t result = 0;
    if (position >= str.size()) {
      return deflt;
    }
    if (str[position] == ',') {
      ++position;
    } else if (!isdigit(str[position])) {
      return deflt;
    }
    for (;;) {
      result = 10 * result + str[position++] - '0';
      if (position == str.size() || !isdigit(str[position])) {
        break;
      }
    }
    return result;
  }

  ArenaSet* const arenas_;
  WaveBufferPtr deviceBuffer_;
  WaveBufferPtr hostBuffer_;
  WaveBufferPtr lookupBuffer_;
  std::unique_ptr<int32_t[]> hostLookup_;
  std::unique_ptr<int32_t[]> hostInts_;
  std::unique_ptr<TestStream> stream_;
  std::unique_ptr<Event> event_;
};

class CudaTest : public testing::Test {
 protected:
  static constexpr int64_t kArenaQuantum = 512 << 20;

  void SetUp() override {
    device_ = getDevice();
    setDevice(device_);
    allocator_ = getAllocator(device_);
    deviceAllocator_ = getDeviceAllocator(device_);
    hostAllocator_ = getHostAllocator(device_);
  }

  void setupMemory(int64_t capacity = 24UL << 30) {
    static bool inited = false;
    if (!globalSyncExecutor) {
      globalSyncExecutor = std::make_unique<folly::CPUThreadPoolExecutor>(10);
    }

    if (inited) {
      return;
    }
    inited = true;
    memory::MemoryManagerOptions options;
    options.useMmapAllocator = true;
    options.allocatorCapacity = capacity;
    memory::MemoryManager::initialize(options);
    manager_ = memory::memoryManager();
  }

  void waitFinish() {
    if (executor_) {
      executor_->join();
    }
    if (globalSyncExecutor) {
      globalSyncExecutor->join();
      globalSyncExecutor = nullptr;
    }
  }

  void streamTest(
      int32_t numStreams,
      int32_t numOps,
      int32_t opSize,
      bool prefetch,
      bool useCallbacks,
      bool syncStreams) {
    int32_t firstNotify = useCallbacks ? 1 : numOps - 1;
    constexpr int32_t kBatch = xsimd::batch<int32_t>::size;
    std::vector<std::unique_ptr<TestStream>> streams;
    std::vector<std::unique_ptr<Event>> events;
    std::vector<int32_t*> ints;
    std::mutex mutex;
    int32_t initValues[16] = {
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    auto initVector = xsimd::load_unaligned(&initValues[0]);
    auto increment = xsimd::broadcast<int32_t>(1);
    std::vector<int64_t> delay;
    delay.reserve(numStreams * (numOps + 2));

    auto start = getCurrentTimeMicro();
    for (auto i = 0; i < numStreams; ++i) {
      streams.push_back(std::make_unique<TestStream>());
      ints.push_back(reinterpret_cast<int32_t*>(
          allocator_->allocate(opSize * sizeof(int32_t))));
      auto last = ints.back();
      auto data = initVector;
      for (auto i = 0; i < opSize; i += kBatch) {
        data.store_unaligned(last + i);
        data += increment;
      }
    }
    for (auto i = 0; i < numStreams; ++i) {
      streams[i]->addCallback([&]() {
        auto d = getCurrentTimeMicro() - start;
        {
          std::lock_guard<std::mutex> l(mutex);
          delay.push_back(d);
        }
      });
      if (prefetch) {
        streams[i]->prefetch(device_, ints[i], opSize * sizeof(int32_t));
      }
    }

    Semaphore sem(0);
    for (auto counter = 0; counter < numOps; ++counter) {
      if (counter > 0 && syncStreams) {
        waitEach(streams, events);
      }
      for (auto i = 0; i < numStreams; ++i) {
        streams[i]->addOne(ints[i], opSize);
        if (counter == 0 || counter >= firstNotify) {
          streams[i]->addCallback([&]() {
            auto d = getCurrentTimeMicro() - start;
            {
              std::lock_guard<std::mutex> l(mutex);
              delay.push_back(d);
            }
            sem.release();
          });
        }
        if (counter == numOps - 1) {
          if (prefetch) {
            streams[i]->prefetch(nullptr, ints[i], opSize * sizeof(int32_t));
          }
        }
      }
      if (syncStreams && counter < numOps - 1) {
        recordEach(streams, events);
      }
    }
    // Destroy the streams while items pending. Items should finish.
    streams.clear();
    for (auto i = 0; i < numStreams * (numOps + 1 - firstNotify); ++i) {
      sem.acquire();
    }
    for (auto i = 0; i < numStreams; ++i) {
      auto* array = ints[i];
      auto data = initVector + numOps;
      xsimd::batch_bool<int32_t> error;
      error = error ^ error;
      for (auto j = 0; j < opSize; j += kBatch) {
        error = error | (data != xsimd::load_unaligned(array + j));
        data += increment;
      }
      ASSERT_EQ(0, simd::toBitMask(error));
      delay.push_back(getCurrentTimeMicro() - start);
    }
    for (auto i = 0; i < numStreams; ++i) {
      allocator_->free(ints[i], sizeof(int32_t) * opSize);
    }
    std::cout << "Delays: ";
    int32_t counter = 0;
    for (auto d : delay) {
      std::cout << d << " ";
      if (++counter % numStreams == 0) {
        std::cout << std::endl;
      }
    }
    std::cout << std::endl;
    float toDeviceMicros = delay[(2 * numStreams) - 1] - delay[0];
    float inDeviceMicros =
        delay[delay.size() - numStreams - 1] - delay[numStreams * 2 - 1];
    float toHostMicros = delay.back() - delay[delay.size() - numStreams];
    float gbSize =
        (sizeof(int32_t) * numStreams * static_cast<float>(opSize)) / (1 << 30);
    std::cout << "to device= " << toDeviceMicros << "us ("
              << gbSize / (toDeviceMicros / 1000000) << " GB/s)" << std::endl;
    std::cout << "In device (ex. first pass): " << inDeviceMicros << "us ("
              << gbSize * (numOps - 1) / (inDeviceMicros / 1000000) << " GB/s)"
              << std::endl;
    std::cout << "to host= " << toHostMicros << "us ("
              << gbSize / (toHostMicros / 1000000) << " GB/s)" << std::endl;
  }

  void recordEach(
      std::vector<std::unique_ptr<TestStream>>& streams,
      std::vector<std::unique_ptr<Event>>& events) {
    for (auto& stream : streams) {
      events.push_back(std::make_unique<Event>());
      events.back()->record(*stream);
    }
  }

  // Every stream waits for every event recorded on each stream in the previous
  // call to recordEach.
  void waitEach(
      std::vector<std::unique_ptr<TestStream>>& streams,
      std::vector<std::unique_ptr<Event>>& events) {
    auto firstEvent = events.size() - streams.size();
    for (auto& stream : streams) {
      for (auto eventIndex = firstEvent; eventIndex < events.size();
           ++eventIndex) {
        events[eventIndex]->wait(*stream);
      }
    }
  }

  void createData(int32_t numBatches, int32_t numColumns, int32_t numRows) {
    batches_.clear();
    if (!batchPool_) {
      batchPool_ = memory::memoryManager()->addLeafPool();
    }
    int32_t sequence = 1;
    for (auto i = 0; i < numBatches; ++i) {
      auto batch = std::make_unique<DataBatch>();
      for (auto j = 0; j < numColumns; ++j) {
        auto buffer = AlignedBuffer::allocate<int64_t>(
            numRows, batchPool_.get(), sequence);
        batch->byteSize += buffer->capacity();
        batch->dataSize += buffer->size();
        batch->columns.push_back(buffer);
        batch->sum += numRows * sequence;
        ++sequence;
      }
      batches_.push_back(std::move(batch));
    }
  }

  DataBatch* getBatch() {
    auto number = ++batchIndex_;
    if (number > batches_.size()) {
      return nullptr;
    }
    return batches_[number - 1].get();
  }

  //
  void processBatches(
      int64_t workingSize,
      GpuArena* unifiedArena,
      GpuArena* deviceArena,
      GpuArena* hostArena,
      std::function<std::unique_ptr<ProcessBatchBase>()> factory,
      RunStats& stats) {
    int64_t pendingSize = 0;
    std::deque<std::unique_ptr<ProcessBatchBase>> work;
    for (;;) {
      int64_t result;
      auto* batch = getBatch();
      if (!batch) {
        for (auto& item : work) {
          item->isReady(result, true);
          stats.resultClocks += item->resultClocks();
          EXPECT_EQ(item->batch()->sum, result);
          processedBytes_ += item->batch()->dataSize;
          pendingSize -= item->batch()->byteSize;
          item.reset();
        }
        return;
      }
      if (pendingSize > workingSize) {
        auto* item = work.front().get();
        item->isReady(result, true);
        pendingSize -= item->batch()->byteSize;
        processedBytes_ += item->batch()->dataSize;
        stats.resultClocks += item->resultClocks();
        EXPECT_EQ(result, item->batch()->sum);
        work.pop_front();
      }
      auto item = factory();
      item->init(batch, unifiedArena, deviceArena, hostArena, executor_.get());
      pendingSize += batch->byteSize;
      work.push_back(std::move(item));
      if (work.front()->isReady(result, false)) {
        EXPECT_EQ(result, work.front()->batch()->sum);
        pendingSize -= work.front()->batch()->byteSize;
        processedBytes_ += work.front()->batch()->dataSize;
        work.pop_front();
      }
    }
  }

  static std::unique_ptr<ProcessBatchBase> makeWork(const std::string& mode) {
    std::unique_ptr<ProcessBatchBase> ptr;
    if (mode == "unified") {
      ptr.reset(new ProcessUnifiedN());
    } else if (mode == "device") {
      ptr.reset(new ProcessUnifiedCudaCopy());
    } else if (mode == "devicecoalesced") {
      ptr.reset(new ProcessDeviceCoalesced());
    } else {
      VELOX_FAIL("Bad mode {}", mode);
    }
    return ptr;
  }

  float reduceTest(
      const std::string& mode,
      int32_t numThreads,
      int64_t workingSize,
      RunStats& stats) {
    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    auto start = getCurrentTimeMicro();
    processedBytes_ = 0;
    batchIndex_ = 0;
    auto factory = [mode]() { return makeWork(mode); };
    for (int32_t i = 0; i < numThreads; ++i) {
      threads.push_back(std::thread([&]() {
        std::unique_ptr<GpuArena> unifiedArena;
        std::unique_ptr<GpuArena> deviceArena;
        std::unique_ptr<GpuArena> hostArena;
        auto arenas = getArenas();
        processBatches(
            workingSize,
            arenas->unified.get(),
            arenas->device.get(),
            arenas->host.get(),
            factory,
            stats);
        releaseArenas(std::move(arenas));
      }));
    }
    for (auto& thread : threads) {
      thread.join();
    }

    auto time = getCurrentTimeMicro() - start;
    float gbs = (processedBytes_ / 1024.0) / time;
    std::cout << time << "us " << gbs << " GB/s"
              << " res clks=" << stats.resultClocks << std::endl;
    stats.gbs = gbs;
    return gbs;
  }

  void roundtripTest(
      const std::string& title,
      const std::vector<std::string>& modeValues,
      bool isCpu,
      int numOps = 10000) {
    auto arenas = getArenas();
    std::vector<RoundtripStats> allStats;
    std::vector<int32_t> numThreadsValues = {2, 4, 8, 16, 32};
    int32_t ordinal = 0;
    for (auto numThreads : numThreadsValues) {
      std::vector<RoundtripStats> runStats;
      for (auto& mode : modeValues) {
        std::vector<std::thread> threads;
        std::vector<std::unique_ptr<RoundtripThread>> runs;
        threads.reserve(numThreads);
        runs.reserve(numThreads);
        std::vector<RoundtripStats> threadStats;
        threadStats.resize(numThreads);

        for (int32_t i = 0; i < numThreads; ++i) {
          threadStats[i].init(++ordinal, isCpu, numThreads, mode, numOps);
          runs.push_back(std::make_unique<RoundtripThread>(0, arenas.get()));
        }
        for (int32_t i = 0; i < numThreads; ++i) {
          threads.push_back(std::thread(
              [i, &runs, &threadStats]() { runs[i]->run(threadStats[i]); }));
        }
        for (auto i = 0; i < numThreads; ++i) {
          threads[i].join();
          if (i == 0) {
            allStats.push_back(threadStats[i]);
          } else {
            allStats.back().add(threadStats[i]);
          }
        }
        allStats.back().micros =
            allStats.back().endMicros - allStats.back().startMicros;
      }
    }
    std::sort(
        allStats.begin(),
        allStats.end(),
        [](const RoundtripStats& left, const RoundtripStats& right) {
          return left.numAdds / left.micros > right.numAdds / right.micros;
        });
    std::cout << std::endl << title << std::endl;
    for (auto& stats : allStats) {
      std::cout << stats.toString() << std::endl;
    }
    releaseArenas(std::move(arenas));
  }

  std::unique_ptr<ArenaSet> getArenas() {
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (!arenas_.empty()) {
        auto value = std::move(arenas_.back());
        arenas_.pop_back();
        return value;
      }
    }
    auto arenas = std::make_unique<ArenaSet>();
    arenas->unified = std::make_unique<GpuArena>(kArenaQuantum, allocator_);
    arenas->device =
        std::make_unique<GpuArena>(kArenaQuantum, deviceAllocator_);
    arenas->host = std::make_unique<GpuArena>(kArenaQuantum, hostAllocator_);
    return arenas;
  }

  void releaseArenas(std::unique_ptr<ArenaSet> arenas) {
    std::lock_guard<std::mutex> l(mutex_);
    arenas_.push_back(std::move(arenas));
  }

  std::shared_ptr<memory::MmapAllocator> mmapAllocator_;
  memory::MemoryManager* manager_{nullptr};
  std::shared_ptr<memory::MemoryPool> batchPool_;
  std::vector<std::unique_ptr<DataBatch>> batches_;
  std::atomic<int32_t> batchIndex_{0};
  std::atomic<int64_t> processedBytes_{0};
  Device* device_;
  GpuAllocator* allocator_;
  GpuAllocator* deviceAllocator_;
  GpuAllocator* hostAllocator_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::vector<RunStats> stats_;
  // Serializes common resources for multithread tests, e.g. 'arenas_'
  std::mutex mutex_;
  std::vector<std::unique_ptr<ArenaSet>> arenas_;
};

TEST_F(CudaTest, stream) {
  constexpr int32_t opSize = 1000000;
  TestStream stream;
  auto ints = reinterpret_cast<int32_t*>(
      allocator_->allocate(opSize * sizeof(int32_t)));
  for (auto i = 0; i < opSize; ++i) {
    ints[i] = i;
  }
  stream.prefetch(device_, ints, opSize * sizeof(int32_t));
  stream.addOne(ints, opSize);
  stream.prefetch(nullptr, ints, opSize * sizeof(int32_t));
  stream.wait();
  for (auto i = 0; i < opSize; ++i) {
    ASSERT_EQ(ints[i], i + 1);
  }
  allocator_->free(ints, sizeof(int32_t) * opSize);
}

TEST_F(CudaTest, callback) {
  streamTest(10, 10, 1024 * 1024, true, false, false);
}

TEST_F(CudaTest, custom) {
  if (FLAGS_num_streams == 0) {
    return;
  }
  streamTest(
      FLAGS_num_streams,
      FLAGS_num_ops,
      FLAGS_op_size,
      FLAGS_prefetch,
      FLAGS_use_callbacks,
      FLAGS_sync_streams);
}

TEST_F(CudaTest, copyReduce) {
  setupMemory();
  executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(64);
  createData(
      FLAGS_num_batches, FLAGS_num_columns, bits::roundUp(FLAGS_num_rows, 256));
  std::vector<std::string> modes = {"unified", "device", "devicecoalesced"};
  bool any = false;
  for (auto& mode : modes) {
    if (FLAGS_mode == "all" || FLAGS_mode == mode) {
      any = true;
      RunStats stats;
      stats.mode = mode;
      stats.numColumns = batches_[0]->columns.size();
      stats.numRows = batches_[0]->columns[0]->size() / sizeof(int64_t);
      stats.numThreads = FLAGS_num_threads;
      stats.workPerThread = FLAGS_working_size / batches_[0]->dataSize;
      reduceTest(mode, FLAGS_num_threads, FLAGS_working_size, stats);
      std::cout << stats.toString() << std::endl;
    }
  }
  if (!any) {
    FAIL() << "Bad mode " << FLAGS_mode;
  }
  waitFinish();
}

TEST_F(CudaTest, reduceMatrix) {
  constexpr int64_t kTestGB = 20;
  if (!FLAGS_enable_bm) {
    return;
  }

  std::vector<std::string> modes = {/*"unified", "device",*/ "devicecoalesced"};
  std::vector<int32_t> batchMBValues = {30, 100};
  std::vector<int32_t> numThreadsValues = {1, 2, 3};
  std::vector<int32_t> workPerThreadValues = {2, 4};
  std::vector<int32_t> numColumnsValues = {10, 100, 300};
  std::vector<int32_t> copyPerThreadValues = {300000, 1000000};
  setupMemory((kTestGB + 1) << 30);
  executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(64);
  for (auto batchMB : batchMBValues) {
    for (auto numColumns : numColumnsValues) {
      auto numBatches = (kTestGB << 30) / (batchMB << 20);
      auto batchSize = (kTestGB << 30) / numBatches / 2;
      auto columnSize = batchSize / numColumns;
      auto numRows = bits::roundUp(columnSize / sizeof(int64_t), kBlockSize);
      batches_.clear();
      createData(numBatches, numColumns, numRows);
      for (int64_t workPerThread : workPerThreadValues) {
        auto workSize =
            workPerThread * batches_[0]->columns[0]->size() * numColumns;
        for (auto numThreads : numThreadsValues) {
          for (auto& mode : modes) {
            if (batchMB <= 10 && numColumns > 10 && mode != "devicecoalesced") {
              continue;
            }
            std::vector<int32_t> zero = {0};
            auto& copySizes =
                mode == "devicecoalesced" ? copyPerThreadValues : zero;
            for (auto copy : copySizes) {
              stats_.emplace_back();
              auto& run = stats_.back();
              run.mode = mode;
              run.numThreads = numThreads;
              run.workPerThread = workPerThread;
              run.numColumns = numColumns;
              run.numRows = numRows;
              run.copyPerThread = copy;
              reduceTest(mode, numThreads, workSize, run);
              std::cout << run.toString() << std::endl;
            }
          }
        }
      }
    }
  }
  std::sort(
      stats_.begin(),
      stats_.end(),
      [](const RunStats& left, const RunStats& right) {
        return left.gbs > right.gbs;
      });
  std::cout << std::endl << "***Result, highest throughput first:" << std::endl;
  for (auto& stats : stats_) {
    std::cout << stats.toString() << std::endl;
  }
  waitFinish();
}

TEST_F(CudaTest, roundtripMatrix) {
  if (!FLAGS_roundtrip_ops.empty()) {
    std::vector<std::string> modes = {FLAGS_roundtrip_ops};
    roundtripTest(
        fmt::format("{} GPU, 1000 repeats", modes[0]), modes, false, 1000);
    roundtripTest(
        fmt::format("{} CPU, 100 repeats", modes[0]), modes, true, 100);
    return;
  }
  if (!FLAGS_enable_bm) {
    return;
  }
  std::vector<std::string> syncModeValues = {
      "dahs",
      "dahe",
      "dsashs",
      "deaehe",
      "whs",
      "d10w10h1sw10h1sw10h1s",
      "d10a10h1sd1a10h1sd1a10h1s"};
  roundtripTest("Sync GPU", syncModeValues, false, 10000);
  roundtripTest("Sync CPU", syncModeValues, true, 10000);

  std::vector<std::string> seqModeValues = {
      "d10h10h10s",
      "d100a100h100s",
      "d1000a1000h1000s",
      "d1000a1000,10h1000s",
      "d1000a1000,10h1sd1a1000,5h1s",
      "d100a100,10h1s",
      "d1000a1000,30h1sd1a1000,30h1s",
      "d1000a1000,150h1sd1a1000,150h1s",
  };
  roundtripTest("Seq GPU", seqModeValues, false, 1024);
  roundtripTest("Seq CPU", seqModeValues, true, 64);

  std::vector<std::string> randomModeValues = {
      "d100r100,10h1s",
      "d100r100,10r100,10h1s",
      "d100r100,10r100,100h1s",
      "d100r100,1000h1s",
      "d1000r1000,10h1s",
      "d1000r1000,100h1s",
      "d10000r10000,10h1s",
      "d30000r30000,50h1s"};
  roundtripTest("Random GPU", randomModeValues, false, 512);
  roundtripTest("Random CPU", randomModeValues, true, 16);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv};
  if (int device; cudaGetDevice(&device) != cudaSuccess) {
    LOG(WARNING) << "No CUDA detected, skipping all tests";
    return 0;
  }
  return RUN_ALL_TESTS();
}
