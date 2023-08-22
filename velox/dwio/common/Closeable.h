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

#pragma once

#include "velox/dwio/common/exception/Exception.h"

namespace facebook {
namespace velox {
namespace dwio {
namespace common {

// Base class for closeable object which need to be explicitly closed before
// being destructed
class Closeable {
 public:
  Closeable() : closed_{false} {}

  virtual ~Closeable() {
    destroy();
  }

  bool isClosed() const {
    return closed_;
  }

  void close() {
    if (!closed_) {
      closed_ = true;
      doClose();
    }
  }

 protected:
  void markClosed() {
    closed_ = true;
  }

  virtual void doClose() {}

  void destroy() {
    if (!closed_) {
      DWIO_WARN_EVERY_N(1000, "close() not called");
      try {
        close();
      } catch (...) {
        DWIO_WARN("failed to call close()");
      }
    }
    DWIO_ENSURE(closed_);
  }

 private:
  bool closed_;
};

} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook
