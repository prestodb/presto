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
#include "presto_cpp/main/CPUMon.h"
#include <cinttypes>
#include "folly/File.h"
#include "folly/FileUtil.h"

namespace facebook::presto {

static bool readProcStat(std::vector<uint64_t>& counters) {
  auto cpuStatFile = folly::File("/proc/stat", O_RDONLY);
  // Enough storage for the /proc/stat CPU data needed below
  std::array<char, 320> buf;
  if (folly::readNoInt(cpuStatFile.fd(), buf.data(), buf.size()) !=
      static_cast<ssize_t>(buf.size())) {
    return false;
  }

  const static char* fmt = "cpu %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64
                           " %" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64;
  if (sscanf(
          buf.data(),
          fmt,
          &counters[0],
          &counters[1],
          &counters[2],
          &counters[3],
          &counters[4],
          &counters[5],
          &counters[6],
          &counters[7]) != static_cast<int>(counters.size())) {
    return false;
  }

  return true;
}

void CPUMon::update() {
  // We do this only for linux, other OS don't have this mechanism.
  // If needed, another mechanism can be added for other OS.
#ifdef __linux__
  double cpuUtil = 0.0;

  // Corner case: When parsing /proc/stat fails, set the cpuUtil to 0.
  std::vector<uint64_t> cur(8);
  if (readProcStat(cur)) {
    if (not firstTime_) {
      /**
       * The values in the /proc/stat is the CPU time since boot.
       * Columns [0, 1, ... 9] map to [user, nice, system, idle, iowait, irq,
       * softirq, steal, guest, guest_nice]. Guest related fields are not used
       * for the cpu util calculation. The total CPU time in the last
       * window is delta busy time over delta total time.
       */
      auto curUtil =
          cur[0] + cur[1] + cur[2] + cur[4] + cur[5] + cur[6] + cur[7];
      auto prevUtil = prev_[0] + prev_[1] + prev_[2] + prev_[4] + prev_[5] +
          prev_[6] + prev_[7];
      auto utilDiff = static_cast<double>(curUtil - prevUtil);
      auto totalDiff = utilDiff + cur[3] - prev_[3];

      /**
       * Corner case: If CPU didn't change or the proc/stat didn't get
       * updated or ticks didn't increase, set the cpuUtil to 0.
       */
      if (totalDiff < 0.001 || curUtil < prevUtil) {
        cpuUtil = 0.0;
      } else {
        // Corner case: The max of CPU utilization can be at most 100%.
        cpuUtil = std::min((utilDiff / totalDiff) * 100, 100.0);
      }
    } else {
      firstTime_ = false;
    }
    prev_ = std::move(cur);
  }
  cpuLoadPct_.store(cpuUtil);
#endif
}

} // namespace facebook::presto
