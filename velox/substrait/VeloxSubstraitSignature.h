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

#include "velox/type/Type.h"

namespace facebook::velox::substrait {

class VeloxSubstraitSignature {
 public:
  /// Given a velox type kind, return the Substrait type signature, throw if no
  /// match found, Substrait signature used in the function extension
  /// declaration is a combination of the name of the function along with a list
  /// of input argument types.The format is as follows : <function
  /// name>:<short_arg_type0>_<short_arg_type1>_..._<short_arg_typeN> for more
  /// detail information about the argument type please refer to link
  /// https://substrait.io/extensions/#function-signature-compound-names.
  static std::string toSubstraitSignature(const TypeKind typeKind);

  /// Given a velox scalar function name and argument types, return the
  /// substrait function signature.
  static std::string toSubstraitSignature(
      const std::string& functionName,
      const std::vector<TypePtr>& arguments);
};

} // namespace facebook::velox::substrait
