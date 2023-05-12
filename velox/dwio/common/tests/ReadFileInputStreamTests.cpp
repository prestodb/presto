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

#include "velox/dwio/common/InputStream.h"

#include <string_view>

#include "gtest/gtest.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;

TEST(ReadFileInputStream, SimpleUsage) {
  std::string fileData;
  {
    InMemoryWriteFile writeFile(&fileData);
    writeFile.append("aaaaa");
    writeFile.append("bbbbb");
    writeFile.append("ccccc");
  }
  auto readFile = std::make_shared<InMemoryReadFile>(fileData);
  ReadFileInputStream inputStream(readFile);
  ASSERT_EQ(inputStream.getLength(), 15);
  auto buf = std::make_unique<char[]>(15);

  inputStream.read(buf.get(), 7, 4, LogType::STREAM);
  std::string_view read_value(buf.get(), 7);
  ASSERT_EQ(read_value, "abbbbbc");

  inputStream.read(buf.get(), 15, 0, LogType::STREAM);
  read_value = {buf.get(), 15};
  ASSERT_EQ(read_value, "aaaaabbbbbccccc");
}

TEST(ReadFileInputStream, VRead) {
  std::string fileData;
  {
    InMemoryWriteFile writeFile(&fileData);
    writeFile.append("aaaaa");
    writeFile.append("bbbbb");
    writeFile.append("ccccc");
  }
  auto readFile = std::make_shared<InMemoryReadFile>(fileData);
  ReadFileInputStream inputStream(readFile);
  ASSERT_EQ(inputStream.getLength(), 15);
  auto buf1 = std::make_unique<char[]>(5);
  auto buf2 = std::make_unique<char[]>(5);
  std::vector<void*> buffers;
  buffers.emplace_back(buf1.get());
  buffers.emplace_back(buf2.get());
  std::vector<Region> regions;
  regions.emplace_back(0, 5);
  regions.emplace_back(10, 5);
  inputStream.vread(buffers, regions, LogType::STREAM);
  std::string_view read_value(buf1.get(), 5);
  ASSERT_EQ(read_value, "aaaaa");
  read_value = {buf2.get(), 5};
  ASSERT_EQ(read_value, "ccccc");
}
