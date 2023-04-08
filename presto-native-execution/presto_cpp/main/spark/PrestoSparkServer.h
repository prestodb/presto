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

#include "presto_cpp/main/PrestoServer.h"

namespace facebook::presto {

/// C++ Presto Server for Presto-on-Spark.
/// Customize this class to be suitable for Presto-on-Spark. However, do not
/// deviate significantly from C++ Presto Server.
/// This server is accessible under the name of `presto_spark_server`.
class PrestoSparkServer : public PrestoServer {
 public:
  explicit PrestoSparkServer(const std::string& configDirectoryPath);

 protected:
  void registerFunctions() override;

  /// Registers all the test function if test mode is enabled
  virtual void registerTestFunctions();
};
} // namespace facebook::presto
