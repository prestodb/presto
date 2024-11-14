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

/*
 * Copyright (c) 2024 by Rivos Inc.
 * Licensed under the Apache License, Version 2.0, see LICENSE for details.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#include <string>

#include "breeze/utils/trace.h"

TRACE_TRACK_EVENT_STATIC_STORAGE();

#if TRACING
using namespace breeze::utils;

class TraceListener : public testing::EmptyTestEventListener {
  void OnTestSuiteStart(const testing::TestSuite& test_suite) override {
    auto type_param = test_suite.type_param();
    if (type_param) {
      TRACE_EVENT_BEGIN("test", perfetto::DynamicString{test_suite.name()},
                        "type_param", type_param);
    } else {
      TRACE_EVENT_BEGIN("test", perfetto::DynamicString{test_suite.name()});
    }
  }
  void OnTestSuiteEnd(const testing::TestSuite&) override {
    TRACE_EVENT_END("test");
  }
  void OnTestStart(const testing::TestInfo& test_info) override {
    TRACE_EVENT_BEGIN("test", perfetto::DynamicString{test_info.name()});
  }
  void OnTestEnd(const testing::TestInfo&) override { TRACE_EVENT_END("test"); }
};
#endif

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

#if TRACING
  std::unique_ptr<perfetto::TracingSession> tracing_session;
  char* tracing = getenv("TRACING");
  bool system = !tracing || strcmp(tracing, "app") != 0;
  initialize_tracing(system);
  if (!system) {
    tracing_session = start_tracing();
  }
  testing::UnitTest::GetInstance()->listeners().Append(new TraceListener);
#endif

  int rv = RUN_ALL_TESTS();

#if TRACING
  flush_tracing();
  if (tracing_session) {
    char* trace_file = getenv("TRACE_FILE");
    std::string default_trace_file = std::string(argv[0]) + ".pftrace";
    stop_tracing(trace_file ? trace_file : default_trace_file.c_str(),
                 std::move(tracing_session));
  }
#endif

  return rv;
}
