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

namespace facebook {
namespace velox {
namespace codegen {

enum class ExpressionNullMode {
  NullInNullOut, // null if any input is null, used for default null analysis
  NullableInNullableOut, // 1. if any input is nullable then output is nullable
                         // 2. it's not default null
  NotNull, // expression never produce null
  Custom // nullability propagation must overridden
};

} // namespace codegen
} // namespace velox
} // namespace facebook
