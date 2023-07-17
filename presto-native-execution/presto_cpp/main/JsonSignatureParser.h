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

#include <string>
#include <unordered_map>
#include "velox/expression/VectorFunction.h"

namespace folly {
struct dynamic;
}

namespace facebook::presto {

/// Class to parse json signature files. It only parses the json and creates
/// the FunctionSignature objects. It does not do the actual registration.
/// It assumes the input json has the following format:
///
///  {
///    "udfSignatureMap": {
///      "my_function": [
///        {
///          "outputType": "varchar",
///          "paramTypes": [
///            "varchar"
///          ],
///          "schema": "my_schema",
///          "routineCharacteristics": {
///					   ...
///			     }
///        },
///      ]
///    }
///  }
///
/// TODO: This json definition only supports scalar signatures for now. It also
/// does not support variadic arguments, type variables, or constant arguments
/// yet.
///
/// This class can be conveniently used in a range for loop:
///
///   for (const auto& it : JsonSignatureParser(jsonString)) {
///     // registration code
///   }
class JsonSignatureParser {
 public:
  using TContainer = std::unordered_map<
      std::string,
      std::vector<velox::exec::FunctionSignaturePtr>>;

  explicit JsonSignatureParser(const std::string& input);

  // Iterator helpers.
  size_t size() const {
    return signaturesMap_.size();
  }

  TContainer::const_iterator begin() const {
    return signaturesMap_.begin();
  }

  TContainer::const_iterator end() const {
    return signaturesMap_.end();
  }

 private:
  /// Parses the top level json parsed.
  void parse(const folly::dynamic& input);

  TContainer signaturesMap_;
};

} // namespace facebook::presto
