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

#pragma once

// Disable TRACING when compiling for device.
#if defined(TRACING) && defined(__CUDA_ARCH__)
#undef TRACING
#endif

#if TRACING

// Workaround for CUDA/HIP PERFETTO_NO_INLINE conflict
#ifdef __noinline__
#undef __noinline__
#define __noinline__ noinline
#endif

#include <perfetto.h>

#include <condition_variable>
#include <fstream>
#include <iostream>
#include <mutex>
#include <type_traits>

// track event categories
PERFETTO_DEFINE_CATEGORIES(
    perfetto::Category("test").SetDescription("Test events"));

#define TRACE_TRACK_EVENT_STATIC_STORAGE(...) \
  PERFETTO_TRACK_EVENT_STATIC_STORAGE()

namespace breeze {
namespace utils {

inline void initialize_tracing(bool system = true) {
  perfetto::TracingInitArgs args;
  args.backends =
      system ? perfetto::kSystemBackend : perfetto::kInProcessBackend;

  perfetto::Tracing::Initialize(args);
  perfetto::TrackEvent::Register();
}

inline std::unique_ptr<perfetto::TracingSession> start_tracing() {
  perfetto::TraceConfig cfg;
  cfg.add_buffers()->set_size_kb(1024);
  auto* ds_cfg = cfg.add_data_sources()->mutable_config();
  ds_cfg->set_name("track_event");

  auto tracing_session = perfetto::Tracing::NewTrace();
  tracing_session->Setup(cfg);
  tracing_session->StartBlocking();
  return tracing_session;
}

inline void stop_tracing(
    const std::string& trace_file,
    std::unique_ptr<perfetto::TracingSession> tracing_session) {
  perfetto::TrackEvent::Flush();

  // stop tracing and read the trace data
  tracing_session->StopBlocking();
  std::vector<char> trace_data(tracing_session->ReadTraceBlocking());

  // write the result into a file
  std::ofstream output;
  output.open(trace_file, std::ios::out | std::ios::binary);
  output.write(&trace_data[0], std::streamsize(trace_data.size()));
  output.close();

  std::cout << "Trace written to " << trace_file << std::endl;
}

inline void flush_tracing() { perfetto::TrackEvent::Flush(); }

}  // namespace utils
}  // namespace breeze

#else  // !TRACING

#define TRACE_TRACK_EVENT_STATIC_STORAGE(...)
#define TRACE_EVENT(...)
#define TRACE_EVENT_BEGIN(...)
#define TRACE_EVENT_END(...)
#define TRACE_EVENT_INSTANT(...)

#endif  // TRACING
