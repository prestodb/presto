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

#pragma once

#include "velox/common/encode/Base64.h"
#include "velox/dwio/common/encryption/Encryption.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook {
namespace velox {
namespace dwio {
namespace common {
namespace encryption {
namespace test {

class TestEncryption {
 public:
  void setKey(const std::string& key) {
    key_ = key;
  }

  const std::string& getKey() const {
    return key_;
  }

  std::unique_ptr<folly::IOBuf> encrypt(folly::StringPiece input) const {
    ++count_;
    auto encoded = velox::encoding::Base64::encodeUrl(input);
    return folly::IOBuf::copyBuffer(key_ + encoded);
  }

  std::unique_ptr<folly::IOBuf> decrypt(folly::StringPiece input) const {
    ++count_;
    std::string key{input.begin(), key_.size()};
    DWIO_ENSURE_EQ(key_, key);
    auto decoded = velox::encoding::Base64::decodeUrl(folly::StringPiece{
        input.begin() + key_.size(), input.size() - key_.size()});
    return folly::IOBuf::copyBuffer(decoded);
  }

  size_t getCount() const {
    return count_;
  }

 private:
  std::string key_;
  mutable size_t count_;
};

class TestEncrypter : public TestEncryption, public Encrypter {
 public:
  const std::string& getKey() const override {
    return TestEncryption::getKey();
  }

  std::unique_ptr<folly::IOBuf> encrypt(
      folly::StringPiece input) const override {
    return TestEncryption::encrypt(input);
  }

  std::unique_ptr<Encrypter> clone() const override {
    auto encrypter = std::make_unique<TestEncrypter>();
    encrypter->setKey(getKey());
    return encrypter;
  }
};

class TestDecrypter : public TestEncryption, public Decrypter {
 public:
  void setKey(const std::string& key) override {
    TestEncryption::setKey(key);
  }

  bool isKeyLoaded() const override {
    return !getKey().empty();
  }

  std::unique_ptr<folly::IOBuf> decrypt(
      folly::StringPiece input) const override {
    return TestEncryption::decrypt(input);
  }

  std::unique_ptr<Decrypter> clone() const override {
    auto decrypter = std::make_unique<TestDecrypter>();
    decrypter->setKey(getKey());
    return decrypter;
  }
};

class TestEncryptionProperties : public EncryptionProperties {
 public:
  TestEncryptionProperties(const std::string& key) : key_{key} {}

  const std::string& getKey() const {
    return key_;
  }

  size_t hash() const override {
    return std::hash<std::string>{}(key_);
  }

 protected:
  bool equals(const EncryptionProperties& other) const override {
    auto& casted = dynamic_cast<const TestEncryptionProperties&>(other);
    return key_ == casted.key_;
  }

 private:
  std::string key_;
};

class TestEncrypterFactory : public EncrypterFactory {
 public:
  std::unique_ptr<Encrypter> create(
      EncryptionProvider provider,
      const EncryptionProperties& props) override {
    DWIO_ENSURE_EQ(provider, EncryptionProvider::Unknown);
    auto encrypter = std::make_unique<TestEncrypter>();
    encrypter->setKey(
        dynamic_cast<const TestEncryptionProperties&>(props).getKey());
    return encrypter;
  }
};

class TestDecrypterFactory : public DecrypterFactory {
 public:
  std::unique_ptr<Decrypter> create(EncryptionProvider provider) override {
    DWIO_ENSURE_EQ(provider, EncryptionProvider::Unknown);
    return std::make_unique<TestDecrypter>();
  }
};

} // namespace test
} // namespace encryption
} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook
