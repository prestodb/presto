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
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include "presto_cpp/main/PrestoServer.h"
#include "presto_cpp/main/common/Utils.h"
#include "velox/common/base/StatsReporter.h"

DEFINE_string(etc_dir, ".", "etc directory for presto configuration");

int main(int argc, char* argv[]) {
  folly::Init init{&argc, &argv};

  google::InstallFailureSignalHandler();
  PRESTO_STARTUP_LOG(INFO) << "Entering main()";
  facebook::presto::PrestoServer presto(FLAGS_etc_dir);
  presto.run();
  PRESTO_SHUTDOWN_LOG(INFO) << "Exiting main()";
}
