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

#include "velox/connectors/hive/FileHandle.h"

#include "velox/common/caching/SimpleLRUCache.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Arena.h"

#include "gtest/gtest.h"

using namespace facebook::velox;

TEST(FileHandle, localFile) {
  // TODO: use the appropriate test directory.
  const char filename[] = "/tmp/test";
  remove(filename);
  {
    LocalWriteFile writeFile(filename);
    writeFile.append("foo");
  }
  FileHandleFactory factory(
      std::make_unique<SimpleLRUCache<std::string, FileHandle>>(1000),
      std::make_unique<FileHandleGenerator>());
  auto fileHandle = factory.generate(filename);
  ASSERT_EQ(fileHandle->file->size(), 3);
  Arena arena;
  ASSERT_EQ(fileHandle->file->pread(0, 3, &arena), "foo");
}
