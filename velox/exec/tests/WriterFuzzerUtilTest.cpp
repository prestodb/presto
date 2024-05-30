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

#include <gtest/gtest.h>
#include "velox/common/file/FileSystems.h"
#include "velox/exec/fuzzer/WriterFuzzer.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

TEST(WriterFuzzerUtilTest, listFolders) {
  facebook::velox::filesystems::registerLocalFileSystem();
  const auto tempFolder = exec::test::TempDirectoryPath::create();
  // Directory layout:
  // First layer:   dir1/   dir2/      dir3/    a
  // Second layer:  b       dir2_1/
  const auto dir1 = fmt::format("{}/dir1", tempFolder->getPath());
  const auto dir2 = fmt::format("{}/dir2", tempFolder->getPath());
  const auto dir2_1 = fmt::format("{}/dir2/dir2_1", tempFolder->getPath());
  const auto dir3 = fmt::format("{}/dir3", tempFolder->getPath());
  const auto a = fmt::format("{}/a", tempFolder->getPath());
  const auto b = fmt::format("{}/dir1/b", tempFolder->getPath());

  auto localFs = filesystems::getFileSystem(a, nullptr);
  {
    localFs->mkdir(dir1);
    localFs->mkdir(dir2);
    localFs->mkdir(dir2_1);
    localFs->mkdir(dir3);
    auto writeFile = localFs->openFileForWrite(a);
    writeFile = localFs->openFileForWrite(b);
  }
  auto folder = listFolders(std::string_view(tempFolder->getPath()));
  std::sort(folder.begin(), folder.end());
  ASSERT_EQ(folder, std::vector<std::string>({dir1, dir2, dir2_1, dir3}));

  localFs->remove(b);
  localFs->remove(dir1);
  folder = listFolders(std::string_view(tempFolder->getPath()));
  std::sort(folder.begin(), folder.end());
  ASSERT_EQ(folder, std::vector<std::string>({dir2, dir2_1, dir3}));

  localFs->remove(dir2_1);
  localFs->remove(dir2);
  localFs->remove(dir3);
  ASSERT_TRUE(listFolders(std::string_view(tempFolder->getPath())).empty());
}
