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

#include <cmath>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"

namespace facebook::velox::type {

class Scanner : public yyFlexLexer {
 public:
  Scanner(
      std::istream& arg_yyin,
      std::ostream& arg_yyout,
      TypePtr& outputType,
      const std::string_view input)
      : yyFlexLexer(&arg_yyin, &arg_yyout),
        outputType_(outputType),
        input_(input){};
  int lex(Parser::semantic_type* yylval);

  void setType(TypePtr type) {
    outputType_ = std::move(type);
  }

  // Store input to print it as part of the error message.
  std::string_view input() {
    return input_;
  }

 private:
  TypePtr& outputType_;
  const std::string_view input_;
};

} // namespace facebook::velox::type
