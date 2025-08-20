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
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/tests/MutableConfigs.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

using namespace facebook::velox;

namespace facebook::presto::test {

void setupMutableSystemConfig() {
  auto dir = exec::test::TempDirectoryPath::create();
  auto sysConfigFilePath = fmt::format("{}/config.properties", dir->getPath());
  auto fileSystem = filesystems::getFileSystem(sysConfigFilePath, nullptr);
  auto sysConfigFile = fileSystem->openFileForWrite(sysConfigFilePath);
  sysConfigFile->append(fmt::format("{}=true\n", ConfigBase::kMutableConfig));
  sysConfigFile->append(
      fmt::format("{}=4GB\n", SystemConfig::kQueryMaxMemoryPerNode));
  sysConfigFile->close();
  SystemConfig::instance()->initialize(sysConfigFilePath);
}

} // namespace facebook::presto::test
