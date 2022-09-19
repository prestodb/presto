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

#include "velox/common/base/Exceptions.h"

#include <cmath>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

namespace facebook::velox::expression::calculate {

class Scanner : public yyFlexLexer {
 public:
  Scanner(
      std::istream& arg_yyin,
      std::ostream& arg_yyout,
      std::unordered_map<std::string, std::optional<int>>& values)
      : yyFlexLexer(&arg_yyin, &arg_yyout), values_(values){};
  int lex(Parser::semantic_type* yylval);

  void setValue(const std::string& varName, int value) {
    values_[varName] = value;
  }

  int getValue(const std::string& varName) const {
    VELOX_CHECK(
        values_.at(varName).has_value(), "Variable {} is not defined", varName);
    return values_.at(varName).value();
  }

 private:
  std::unordered_map<std::string, std::optional<int>>& values_;
};

} // namespace facebook::velox::expression::calculate
