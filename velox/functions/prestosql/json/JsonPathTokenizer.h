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

#include <optional>
#include <string>

namespace facebook::velox::functions {

class JsonPathTokenizer {
 public:
  bool reset(std::string_view path);

  bool hasNext() const;

  std::optional<std::string> getNext();

 private:
  bool match(char expected);

  std::optional<std::string> matchDotKey();

  std::optional<std::string> matchUnquotedSubscriptKey();

  std::optional<std::string> matchQuotedSubscriptKey();

 private:
  size_t index_;
  std::string_view path_;
};

} // namespace facebook::velox::functions
