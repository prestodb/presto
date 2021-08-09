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

#pragma once

#include <iostream>

#if !defined(CODEGEN_PERF) || !defined(__linux__)
namespace facebook::velox::codegen {
class Perf {
 public:
  Perf() {}
  template <typename Seq>
  explicit Perf(const Seq&) {}

  template <typename PerfEvent>
  [[maybe_unused]] void addEvent(PerfEvent) {}
  void start() const {}
  void stop() const {}
  void reset() const {}
  void report() const {}
  void report(std::ostream&) const {}
};
} // namespace facebook::velox::codegen
#else

#include <linux/perf_event.h>
#include <stdint.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <list>
#include <vector>

const inline std::string hw_events[] = {
    "cpu cycles",
    "instructions",
    "cache references",
    "cache misses",
    "branch instructions",
    "branch misses",
    "bus cycles",
    "stalled cycles frontend",
    "stalled cycles backend",
    "ref cpu cycles",
};

const inline std::string sw_events[] = {
    "cpu clock",
    "task clock",
    "page faults",
    "context switches",
    "cpu migrations",
    "page faults minor",
    "page faults major",
    "alignment faults",
    "emulation faults",
    "dummy",
    "BPF output",
};

const inline std::string cache_events[] = {
    "l1d",
    "l1i",
    "ll",
    "dtlb",
    "itlb",
    "bpu",
    "node",
};

const inline std::string cache_ops[] = {
    "read",
    "write",
    "prefetch",
};

const inline std::string cache_results[] = {
    "access",
    "miss",
};

struct PerfEvent {
  uint32_t type;
  uint32_t config;
};

inline std::vector<PerfEvent> defaultEvents = {
    //    {PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES},
    //    {PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS},
    {PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_INSTRUCTIONS},
    {PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES},
    //    {PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_REFERENCES},
    //    {PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES},
    {PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CPU_CLOCK},
    {PERF_TYPE_SOFTWARE, PERF_COUNT_SW_TASK_CLOCK},
};

inline std::ostream& operator<<(std::ostream& os, const PerfEvent& e) {
  switch (e.type) {
    case PERF_TYPE_HARDWARE:
      return os << hw_events[e.config];
    case PERF_TYPE_SOFTWARE:
      return os << sw_events[e.config];
    case PERF_TYPE_HW_CACHE:
      return os << cache_events[e.config & 0xff] << "_"
                << cache_ops[(e.config >> 8) & 0xff] << "_"
                << cache_results[e.config >> 16];
    default:
      return os << "Unknown event";
  }
}

namespace facebook {
namespace velox {
namespace codegen {

class Perf {
 public:
  Perf() : Perf(defaultEvents) {}
  template <typename EventList>
  explicit Perf(const EventList& eventList) : leader_(-1) {
    for (const PerfEvent& e : eventList) {
      addEvent(e);
    }
    reset();
    start();
  }

  ~Perf() {
    stop();
    report();
    for (auto& [fd, _] : events_) {
      close(fd);
    }
  }

  void addEvent(PerfEvent e) {
    events_.emplace_back(-1, (perf_event_attr){});
    auto& [fd, pe] = events_.back();
    memset(&pe, 0, sizeof(pe));
    pe.type = e.type;
    pe.config = e.config;
    pe.size = sizeof(pe);
    pe.disabled = 1; // disabled at the beginning
    pe.exclude_kernel = 1; // only user space event
    pe.exclude_hv = 1; // exclude hypervisor event (if any)
    pe.inherit = 1; // count child tasks (if any)
    pe.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED |
        PERF_FORMAT_TOTAL_TIME_RUNNING; // used if PMU is overcommited

    fd = syscall(SYS_perf_event_open, &pe, 0, -1, leader_, 0);

    if (fd == -1) {
      std::cerr << "Unsupported event: " << e << std::endl;
      events_.pop_back();
      return;
    }

    if (leader_ == -1) {
      leader_ = fd;
    }
  }

  // do we need a removeEvent?

  void start() const {
    ioctl(leader_, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
  }

  void stop() const {
    ioctl(leader_, PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
  }

  void reset() const {
    ioctl(leader_, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
  }

  void report(std::ostream& os = std::cerr) const {
    ReadFormat result;
    for (auto& [fd, e] : events_) {
      read(fd, &result, sizeof(result));
      uint64_t val = result.val;
      if (result.timeEnabled != result.timeRunning) {
        os << "WARNING: PMU overcommitted, the result below is only an"
              " estimation and may not be sane depending on the hardware"
           << std::endl;
        val = result.val * (result.timeRunning / (double)result.timeEnabled);
      }
      os << (PerfEvent){e.type, (uint32_t)e.config} << ": " << val << std::endl;
    }
  }

 private:
  struct __attribute__((packed)) ReadFormat {
    uint64_t val; // value of event
    uint64_t timeEnabled; // if PERF_FORMAT_TOTAL_TIME_ENABLED
    uint64_t timeRunning; // if PERF_FORMAT_TOTAL_TIME_RUNNING
  };
  int leader_; // the fd to toggle on/off all events
  std::list<std::pair<int, perf_event_attr>> events_;
};

} // namespace codegen
} // namespace velox
} // namespace facebook

#endif // !defined(CODEGEN_PERF) || !defined(__linux__)
