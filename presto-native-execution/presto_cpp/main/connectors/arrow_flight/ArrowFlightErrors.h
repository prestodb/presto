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

#include <string_view>

namespace arrow {
class Status;
}

namespace facebook::presto {

/// Classifies a failed Flight call into a stable metric category.
std::string_view errorCategory(const arrow::Status& status);

/// Classifies the exception currently being handled into a stable metric
/// category. Must be called from within a catch block.
std::string_view currentExceptionCategory();

/// Records the process-level counter matching a category returned by
/// errorCategory() or currentExceptionCategory().
void recordCategoryCounter(std::string_view category);

/// Throws a typed Velox exception for a failed Flight call, honoring Presto
/// error metadata attached by the server when present.
[[noreturn]] void raiseFlightError(const arrow::Status& status);

} // namespace facebook::presto
