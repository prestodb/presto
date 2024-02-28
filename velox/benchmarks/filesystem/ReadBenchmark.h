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

#include <iostream>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <folly/Random.h>
#include <folly/Synchronized.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/executors/QueuedImmediateExecutor.h>
#include <folly/futures/Future.h>
#include <folly/init/Init.h>
#include <folly/portability/SysUio.h>
#include <gflags/gflags.h>

#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/time/Timer.h"

DECLARE_string(path);
DECLARE_int64(file_size_gb);
DECLARE_int32(num_threads);
DECLARE_int32(seed);
DECLARE_bool(odirect);
DECLARE_int32(bytes);
DECLARE_int32(gap);
DECLARE_int32(num_in_run);

DECLARE_int32(measurement_size);
DECLARE_string(config);

namespace facebook::velox {

enum class Mode { Pread = 0, Preadv = 1, Multiple = 2 };

// Struct to read data into. If we read contiguous and then copy to
// non-contiguous buffers, we read to 'buffer' and copy to
// 'bufferCopy'.
struct Scratch {
  std::string buffer;
  std::string bufferCopy;
};

// This class implements functions that aid in measuring the throughput of a
// FileSystem for various ReadFile APIs.
class ReadBenchmark {
 public:
  virtual ~ReadBenchmark() = default;

  virtual void initialize();

  virtual void finalize();

  void clearCache() {
#ifdef linux
    // system("echo 3 >/proc/sys/vm/drop_caches");
    bool success = false;
    auto fd = open("/proc//sys/vm/drop_caches", O_WRONLY);
    if (fd > 0) {
      success = write(fd, "3", 1) == 1;
      close(fd);
    }
    if (!success) {
      LOG(ERROR) << "Failed to clear OS disk cache: errno=" << errno;
    }
#endif
  }

  Scratch& getScratch(int32_t size) {
    auto scratch = scratch_.withWLock([&](auto& table) {
      auto& ptr = table[std::this_thread::get_id()];
      if (!ptr) {
        ptr = std::make_unique<Scratch>();
      }
      ptr->buffer.resize(size);
      ptr->bufferCopy.resize(size);
      return ptr.get();
    });
    return *scratch;
  }

  // Measures the throughput for various ReadFile APIs(modes).
  void randomReads(
      int32_t size,
      int32_t gap,
      int32_t count,
      int32_t repeats,
      Mode mode,
      bool parallel) {
    clearCache();
    std::vector<folly::Promise<bool>> promises;
    std::vector<folly::SemiFuture<bool>> futures;
    uint64_t usec = 0;
    std::string label;
    {
      MicrosecondTimer timer(&usec);
      int32_t rangeSize = size * count + gap * (count - 1);
      auto& globalScratch = getScratch(rangeSize);
      globalScratch.buffer.resize(rangeSize);
      globalScratch.bufferCopy.resize(rangeSize);
      for (auto repeat = 0; repeat < repeats; ++repeat) {
        std::unique_ptr<folly::Promise<bool>> promise;
        if (parallel) {
          auto [tempPromise, future] = folly::makePromiseContract<bool>();
          promise = std::make_unique<folly::Promise<bool>>();
          *promise = std::move(tempPromise);
          futures.push_back(std::move(future));
        }
        int64_t offset = folly::Random::rand64(rng_) % (fileSize_ - rangeSize);
        switch (mode) {
          case Mode::Pread:
            label = "1 pread";
            if (parallel) {
              executor_->add([offset,
                              gap,
                              size,
                              count,
                              rangeSize,
                              this,
                              capturedPromise = std::move(promise)]() {
                auto& scratch = getScratch(rangeSize);
                readFile_->pread(offset, rangeSize, scratch.buffer.data());
                for (auto i = 0; i < count; ++i) {
                  memcpy(
                      scratch.bufferCopy.data() + i * size,
                      scratch.buffer.data() + i * (size + gap),
                      size);
                }
                capturedPromise->setValue(true);
              }

              );
            } else {
              readFile_->pread(offset, rangeSize, globalScratch.buffer.data());
              for (auto i = 0; i < count; ++i) {
                memcpy(
                    globalScratch.bufferCopy.data() + i * size,
                    globalScratch.buffer.data() + i * (size + gap),
                    size);
              }
            }
            break;
          case Mode::Preadv: {
            label = "1 preadv";
            if (parallel) {
              executor_->add([offset,
                              gap,
                              size,
                              rangeSize,
                              this,
                              capturedPromise = std::move(promise)]() {
                auto& scratch = getScratch(rangeSize);
                std::vector<folly::Range<char*>> ranges;
                for (auto start = 0; start < rangeSize; start += size + gap) {
                  ranges.push_back(
                      folly::Range<char*>(scratch.buffer.data() + start, size));
                  if (gap && start + gap < rangeSize) {
                    ranges.push_back(folly::Range<char*>(nullptr, gap));
                  }
                }
                readFile_->preadv(offset, ranges);
                capturedPromise->setValue(true);
              });
            } else {
              std::vector<folly::Range<char*>> ranges;
              for (auto start = 0; start < rangeSize; start += size + gap) {
                ranges.push_back(folly::Range<char*>(
                    globalScratch.buffer.data() + start, size));
                if (gap && start + gap < rangeSize) {
                  ranges.push_back(folly::Range<char*>(nullptr, gap));
                }
              }
              readFile_->preadv(offset, ranges);
            }

            break;
          }
          case Mode::Multiple: {
            label = "multiple pread";
            if (parallel) {
              executor_->add([offset,
                              gap,
                              size,
                              count,
                              rangeSize,
                              this,
                              capturedPromise = std::move(promise)]() {
                auto& scratch = getScratch(rangeSize);
                for (auto counter = 0; counter < count; ++counter) {
                  readFile_->pread(
                      offset + counter * (size + gap),
                      size,
                      scratch.buffer.data() + counter * size);
                }
                capturedPromise->setValue(true);
              });
            } else {
              for (auto counter = 0; counter < count; ++counter) {
                readFile_->pread(
                    offset + counter * (size + gap),
                    size,
                    globalScratch.buffer.data() + counter * size);
              }
            }
            break;
          }
        }
      }
      if (parallel) {
        auto& exec = folly::QueuedImmediateExecutor::instance();
        for (int32_t i = futures.size() - 1; i >= 0; --i) {
          std::move(futures[i]).via(&exec).wait();
        }
      }
    }
    std::cout << fmt::format(
                     "{} MB/s {} {}",
                     (static_cast<float>(count) * size * repeats) / usec,
                     label,
                     parallel ? "mt" : "")
              << std::endl;
  }

  void modes(int32_t size, int32_t gap, int32_t count) {
    int repeats =
        std::max<int32_t>(3, (FLAGS_measurement_size) / (size * count));
    std::cout << fmt::format(
                     "Run: {} Gap: {} Count: {} Repeats: {}",
                     size,
                     gap,
                     count,
                     repeats)
              << std::endl;
    randomReads(size, gap, count, repeats, Mode::Pread, false);
    randomReads(size, gap, count, repeats, Mode::Preadv, false);
    randomReads(size, gap, count, repeats, Mode::Multiple, false);
    randomReads(size, gap, count, repeats, Mode::Pread, true);
    randomReads(size, gap, count, repeats, Mode::Preadv, true);
    randomReads(size, gap, count, repeats, Mode::Multiple, true);
  }

  void run();

 protected:
  static constexpr int64_t kRegionSize = 64 << 20; // 64MB
  static constexpr int32_t kWrite = -10000;
  // 0 means no op, kWrite means being written, other numbers are reader counts.
  std::string writeBatch_;
  int32_t fd_;
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
  std::unique_ptr<ReadFile> readFile_;
  folly::Random::DefaultGenerator rng_;
  int64_t fileSize_;

  folly::Synchronized<
      std::unordered_map<std::thread::id, std::unique_ptr<Scratch>>>
      scratch_;
};

} // namespace facebook::velox
