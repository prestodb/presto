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
#include <sstream>
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox;

class PrestoOutputStreamListenerTest : public ::testing::Test {};

TEST_F(PrestoOutputStreamListenerTest, basic) {
  std::stringstream out;
  serializer::presto::PrestoOutputStreamListener streamlListener;
  auto os = std::make_unique<OStreamOutputStream>(&out, &streamlListener);

  std::string str1 = "this";
  std::string str2 = "is";
  std::string str3 = "hello world!";
  int length = str1.size() + str2.size() + str3.size();
  char codec = 4;
  int skipValue1 = 480;
  int skipValue2 = 8999;

  auto listener = dynamic_cast<serializer::presto::PrestoOutputStreamListener*>(
      os->listener());
  EXPECT_TRUE(listener != nullptr);

  boost::crc_32_type crc;
  crc.process_bytes(&codec, sizeof(codec));
  crc.process_bytes(&length, sizeof(length));
  crc.process_bytes(str1.data(), str1.size());
  crc.process_bytes(str2.data(), str2.size());
  crc.process_bytes(str3.data(), str3.size());

  // We run following tests twice to test listener's reset capability.
  for (int i = 0; i < 2; ++i) {
    listener->reset();
    listener->resume();
    os->write(&codec, sizeof(codec));

    listener->pause();
    // Following two writes won't be reflected towards CRC computation.
    os->write(reinterpret_cast<char*>(&skipValue1), sizeof(skipValue1));
    os->write(reinterpret_cast<char*>(&skipValue2), sizeof(skipValue2));

    listener->resume();
    os->write(reinterpret_cast<char*>(&length), sizeof(length));
    os->write(str1.data(), str1.size());
    os->write(str2.data(), str2.size());
    os->write(str3.data(), str3.size());

    listener->pause();
    // Following two writes won't be reflected towards CRC computation.
    os->write(reinterpret_cast<char*>(&skipValue1), sizeof(skipValue1));
    os->write(reinterpret_cast<char*>(&skipValue2), sizeof(skipValue2));

    EXPECT_EQ(crc.checksum(), listener->crc().checksum());
  }
}
