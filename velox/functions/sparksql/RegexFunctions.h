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
#include <memory>
#include <vector>

#include "velox/expression/Expr.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::functions::sparksql {

// These functions delegate to the RE2-based implementations in
// lib/Re2Functions.h, but check to ensure that syntax that has different
// semantics between Spark (which uses java.util.regex) and RE2 throws an
// error.
//
// Some incompatibilities:
// - \h, \H, \v, \V - horizontal and vertical whitespace classes
// - \s - in RE2, does not include \x0B (vertical tab) unlike java.util.
// - character class union, intersection and difference ([a[b]], [a&&[b]] and
//   [a&&[^b]])
// - \uHHHH for four-digit hex code HHHH
// - \e for escape (\033)
// - \cK for control character K
// - \p{X} - supported Unicode character class names differ
// - \G - end of previous match
// - \Z - end of the input except the final terminator(?)
// - \R - any linebreak
// - Features related to backtracking (which RE2 explicitly does not support):
//   - Backreferences
//   - Possessive quantifiers (these disable backtracking)
//   - Lookahead (?=...), (?!...), (?<=...), (?<!...)
//   - (?>...) as an independent non-capturing group (i.e. backtracking
//     disabled)
//
// Character class set operations are the only case where re2 would interpret
// the regex differently than Spark expects, so we throw an error on these
// patterns. At the moment this is implemented in a way that requires the
// pattern is a constant.

std::shared_ptr<exec::VectorFunction> makeRLike(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs);

std::shared_ptr<exec::VectorFunction> makeRegexExtract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs);

} // namespace facebook::velox::functions::sparksql
