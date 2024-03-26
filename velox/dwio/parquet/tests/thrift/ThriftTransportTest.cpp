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

#include "velox/dwio/parquet/thrift/ThriftTransport.h"
#include <folly/init/Init.h>
#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet::thrift;

class ThriftTransportTest : public testing::Test {
 protected:
  void SetUp() override {
    input_.resize(bufferSize_);
    output_.resize(bufferSize_);
    for (size_t i = 0; i < input_.size(); ++i) {
      input_[i] = static_cast<uint8_t>(i);
    }
  }

  void prepareThriftStreamingTransport() {
    inputStream_ = std::make_shared<SeekableArrayInputStream>(
        input_.data(), input_.size(), 20);
    int32_t batchSize_;
    const void* bufferPointer;
    if (!inputStream_->Next(&bufferPointer, &batchSize_)) {
      VELOX_CHECK(false, "Reading past end");
    }
    bufferStart_ = static_cast<const char*>(bufferPointer);
    bufferEnd_ = bufferStart_ + batchSize_;
    transport_ = std::make_shared<ThriftStreamingTransport>(
        inputStream_.get(), bufferStart_, bufferEnd_);
  }

  void prepareThriftBufferedTransport() {
    transport_ =
        std::make_shared<ThriftBufferedTransport>(input_.data(), bufferSize_);
  }

  static constexpr uint32_t bufferSize_ = 200;
  static constexpr uint32_t batchSize_ = 20;
  std::vector<uint8_t> input_;
  std::vector<uint8_t> output_;
  const char* bufferStart_{nullptr};
  const char* bufferEnd_{nullptr};
  std::shared_ptr<SeekableInputStream> inputStream_;
  std::shared_ptr<ThriftTransport> transport_;
};

TEST_F(ThriftTransportTest, streaming) {
  prepareThriftStreamingTransport();
  transport_->read(output_.data(), 10);
  transport_->read(output_.data() + 10, 50);
  transport_->read(output_.data() + 60, 140);

  for (size_t i = 0; i < input_.size(); ++i) {
    VELOX_CHECK_EQ(input_[i], output_[i]);
  }
}

TEST_F(ThriftTransportTest, streamingOutOfBoundry) {
  prepareThriftStreamingTransport();
  transport_->read(output_.data(), 10);
  transport_->read(output_.data() + 10, 50);
  transport_->read(output_.data() + 60, 140);

  // The whole inputStream_ is consumed.
  EXPECT_ANY_THROW(transport_->read(output_.data() + bufferSize_, 1));
}

TEST_F(ThriftTransportTest, buffered) {
  prepareThriftBufferedTransport();
  transport_->read(output_.data(), 10);
  transport_->read(output_.data() + 10, 50);
  transport_->read(output_.data() + 60, 140);

  for (size_t i = 0; i < input_.size(); ++i) {
    VELOX_CHECK_EQ(input_[i], output_[i]);
  }
}

TEST_F(ThriftTransportTest, bufferedOutOfBoundry) {
  prepareThriftStreamingTransport();
  transport_->read(output_.data(), 10);
  transport_->read(output_.data() + 10, 50);
  transport_->read(output_.data() + 60, 140);

  // The whole inputStream_ is consumed.
  EXPECT_ANY_THROW(transport_->read(output_.data() + bufferSize_, 1));
}

// Define main so that gflags get processed.
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
