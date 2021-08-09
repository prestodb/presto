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

#include <iostream>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include "velox/experimental/codegen/external_process/Command.h"
#include "velox/experimental/codegen/external_process/subprocess.h"

using namespace facebook::velox::codegen::external_process;
TEST(ExternalProcess, Command) {
  Command command{"/bin/ls", {"-l", "-a"}};
  ASSERT_EQ(command.toString(), "/bin/ls\n-l\n-a");
}

TEST(ExternalProcess, launchCommand) {
  Command command{"/bin/ls", {"-l", "-a"}};
  std::filesystem::path out, err;
  auto proc = launchCommand(command, out, err);
  proc.wait();
  ASSERT_EQ(proc.exit_code(), 0);
  ASSERT_GT(std::filesystem::file_size(out), 0);
}
