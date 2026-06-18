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

#include <cstdint>

namespace facebook::presto::http {

struct HttpClientOptions {
  // HTTP/2 transport settings (used in constructor)
  bool http2Enabled{false};
  uint32_t http2MaxStreamsPerConnection{8};
  uint32_t http2InitialStreamWindow{1 << 23};
  uint32_t http2StreamWindow{1 << 23};
  uint32_t http2SessionWindow{1 << 26};
  uint64_t maxAllocateBytes{65536};

  // Metrics settings (used in ResponseHandler)
  bool connectionReuseCounterEnabled{true};
};

} // namespace facebook::presto::http
