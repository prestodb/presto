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

#include "velox/connectors/hive/FileHandle.h"

#include "gtest/gtest.h"
#include "velox/common/caching/SimpleLRUCache.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/TempFilePath.h"

using namespace facebook::velox;

TEST(FileHandleTest, localFile) {
  filesystems::registerLocalFileSystem();

  auto tempFile = exec::test::TempFilePath::create();
  const auto& filename = tempFile->getPath();
  remove(filename.c_str());

  {
    LocalWriteFile writeFile(filename);
    writeFile.append("foo");
  }

  FileHandleFactory factory(
      std::make_unique<SimpleLRUCache<FileHandleKey, FileHandle>>(1000),
      std::make_unique<FileHandleGenerator>());
  FileHandleKey key{filename};
  auto fileHandle = factory.generate(key);
  ASSERT_EQ(fileHandle->file->size(), 3);
  char buffer[3];
  ASSERT_EQ(fileHandle->file->pread(0, 3, &buffer), "foo");

  // Clean up
  remove(filename.c_str());
}

TEST(FileHandleTest, localFileWithProperties) {
  filesystems::registerLocalFileSystem();

  auto tempFile = exec::test::TempFilePath::create();
  const auto& filename = tempFile->getPath();
  remove(filename.c_str());

  {
    LocalWriteFile writeFile(filename);
    writeFile.append("foo");
  }

  FileHandleFactory factory(
      std::make_unique<SimpleLRUCache<FileHandleKey, FileHandle>>(1000),
      std::make_unique<FileHandleGenerator>());
  FileProperties properties = {
      .fileSize = tempFile->fileSize(),
      .modificationTime = tempFile->fileModifiedTime()};
  FileHandleKey key{filename};
  auto fileHandle = factory.generate(key, &properties);
  ASSERT_EQ(fileHandle->file->size(), 3);
  char buffer[3];
  ASSERT_EQ(fileHandle->file->pread(0, 3, &buffer), "foo");

  // Clean up
  remove(filename.c_str());
}
