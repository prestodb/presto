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

#include <string>

namespace facebook::velox::test {

/// Utility class that helps to run any expressions standalone. It takes input
/// data, SQL, and dirty result vector if any from disk and run the expression
/// described by SQL. It supports 3 modes:
///    - "verify": run expression and compare results between common and
///                simplified path
///    - "common": run expression only using common paths. (to be supported)
///    - "simplified": run expression only using simplified path. (to be
///                supported)
class ExpressionRunner {
 public:
  /// \param inputPath The path to the on-disk vector that will be used as input
  ///        to feed to the expression.
  /// \param sqlPath The path to the on-disk SQL that will be used to describe
  ///        the expression. \param resultPath The path to the on-disk vector
  ///        that will be used as the result buffer to which the expression
  ///        evaluation results will be written.
  /// \param mode The expression evaluation mode, one of ["verify", "common",
  ///        "simplified"]
  ///
  /// User can refer to 'VectorSaver' class to see how to serialize/preserve
  /// vectors to disk.
  static void run(
      const std::string& inputPath,
      const std::string& sqlPath,
      const std::string& resultPath,
      const std::string& mode);
};

} // namespace facebook::velox::test
