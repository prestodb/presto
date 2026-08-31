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
#include "presto_cpp/main/operators/ShuffleInterface.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::presto::operators {

void ShuffleWriter::collect(
    int32_t partition,
    std::string_view key,
    std::unique_ptr<folly::IOBuf> data) {
  VELOX_CHECK_NOT_NULL(data);
  data->coalesce();
  collect(
      partition,
      key,
      std::string_view(
          reinterpret_cast<const char*>(data->data()), data->length()));
}

} // namespace facebook::presto::operators
