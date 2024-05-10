/*
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

#include "presto_cpp/main/PeriodicMemoryChecker.h"

namespace facebook::presto {
class LinuxMemoryChecker : public PeriodicMemoryChecker {
 public:
  explicit LinuxMemoryChecker(
      PeriodicMemoryChecker::Config config,
      int64_t mallocBytes = 0,
      std::function<void()>&& periodicCb = nullptr,
      std::function<bool(const std::string&)>&& heapDumpCb = nullptr)
      : PeriodicMemoryChecker(config),
        mallocBytes_(mallocBytes),
        periodicCb_(std::move(periodicCb)),
        heapDumpCb_(std::move(heapDumpCb)) {}

  ~LinuxMemoryChecker() override {}

  void setMallocBytes(int64_t mallocBytes) {
    mallocBytes_ = mallocBytes;
  }

 protected:
  int64_t systemUsedMemoryBytes() override;

  int64_t mallocBytes() const override {
    return mallocBytes_;
  }

  void periodicCb() const override {
    if (periodicCb_) {
      periodicCb_();
    }
  }

  bool heapDumpCb(const std::string& filePath) const override {
    if (heapDumpCb_) {
      return heapDumpCb_(filePath);
    }
    return false;
  }

  void removeDumpFile(const std::string& filePath) const override {}

 private:
  int64_t mallocBytes_{0};
  std::function<void()> periodicCb_;
  std::function<bool(const std::string&)> heapDumpCb_;
};
} // namespace facebook::presto
