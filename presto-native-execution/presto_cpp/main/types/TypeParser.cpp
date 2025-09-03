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
#include <boost/algorithm/string.hpp>
#include <iostream>

#include "presto_cpp/main/types/TypeParser.h"
#include "velox/functions/prestosql/types/parser/TypeParser.h"

#include "presto_cpp/main/common/Configs.h"

namespace facebook::presto {

velox::TypePtr TypeParser::parse(const std::string& text) const {
  if (SystemConfig::instance()->charNToVarcharImplicitCast()) {
    if (text.find("char(") == 0 || text.find("CHAR(") == 0) {
      return velox::VARCHAR();
    }
  }

  auto it = cache_.find(text);
  if (it != cache_.end()) {
    return it->second;
  }

  auto result = velox::functions::prestosql::parseType(text);
  cache_.insert({text, result});
  return result;
}
} // namespace facebook::presto
