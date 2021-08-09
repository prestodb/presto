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

#include "velox/dwio/common/exception/Exception.h"
#include "velox/common/base/Exceptions.h"

namespace facebook {
namespace dwio {
namespace common {
namespace exception {

std::unique_ptr<ExceptionLogger>& exceptionLogger() {
  static std::unique_ptr<ExceptionLogger> logger(nullptr);
  return logger;
}

bool registerExceptionLogger(std::unique_ptr<ExceptionLogger> logger) {
  VELOX_CHECK(
      exceptionLogger().get() == nullptr,
      "Exception logger is already registered");
  exceptionLogger() = std::move(logger);
  return true;
}

ExceptionLogger* getExceptionLogger() {
  return exceptionLogger().get();
}

} // namespace exception
} // namespace common
} // namespace dwio
} // namespace facebook
