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

#include <fmt/format.h>

#include "presto_cpp/main/tvf/spi/TableFunction.h"
#include "velox/connectors/Connector.h"

namespace facebook::presto::tvf {

struct TableFunctionSplit : public velox::connector::ConnectorSplit {
  explicit TableFunctionSplit(const TableSplitHandlePtr& handle)
      : ConnectorSplit(""), splitHandle_(handle) {}

  const TableSplitHandlePtr splitHandle() {
    return splitHandle_;
  }

 private:
  const TableSplitHandlePtr splitHandle_;
};

} // namespace facebook::presto::tvf

template <>
struct fmt::formatter<facebook::presto::tvf::TableFunctionSplit>
    : formatter<std::string> {
  auto format(
      facebook::presto::tvf::TableFunctionSplit s,
      format_context& ctx) {
    return formatter<std::string>::format(s.toString(), ctx);
  }
};

template <>
struct fmt::formatter<
    std::shared_ptr<facebook::presto::tvf::TableFunctionSplit>>
    : formatter<std::string> {
  auto format(
      std::shared_ptr<facebook::presto::tvf::TableFunctionSplit> s,
      format_context& ctx) const {
    return formatter<std::string>::format(s->toString(), ctx);
  }
};
