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

#include "folly/Range.h"
#include "folly/io/IOBuf.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook {
namespace dwio {
namespace common {
namespace encryption {

enum class EncryptionProvider { Unknown = 0, CryptoService };

class EncryptionProperties {
 public:
  virtual ~EncryptionProperties() = default;

  virtual size_t hash() const = 0;

 protected:
  virtual bool equals(const EncryptionProperties& other) const = 0;

  friend bool operator==(
      const EncryptionProperties&,
      const EncryptionProperties&);
};

bool operator==(const EncryptionProperties& a, const EncryptionProperties& b);

struct EncryptionPropertiesHash {
  std::size_t operator()(const EncryptionProperties* ep) const {
    return ep->hash();
  }
};

struct EncryptionPropertiesEqual {
  bool operator()(const EncryptionProperties* a, const EncryptionProperties* b)
      const {
    return *a == *b;
  }
};

class Encrypter {
 public:
  virtual ~Encrypter() = default;

  virtual const std::string& getKey() const = 0;

  virtual std::unique_ptr<folly::IOBuf> encrypt(
      folly::StringPiece input) const = 0;

  virtual std::unique_ptr<Encrypter> clone() const = 0;
};

class EncrypterFactory {
 public:
  virtual ~EncrypterFactory() = default;

  virtual std::unique_ptr<Encrypter> create(
      EncryptionProvider provider,
      const EncryptionProperties& props) = 0;
};

class Decrypter {
 public:
  virtual ~Decrypter() = default;

  virtual void setKey(const std::string& key) = 0;

  virtual bool isKeyLoaded() const = 0;

  virtual std::unique_ptr<folly::IOBuf> decrypt(
      folly::StringPiece input) const = 0;

  virtual std::unique_ptr<Decrypter> clone() const = 0;
};

class DecrypterFactory {
 public:
  virtual ~DecrypterFactory() = default;

  virtual std::unique_ptr<Decrypter> create(EncryptionProvider provider) = 0;
};

class DummyDecrypter : public Decrypter {
 public:
  void setKey(const std::string& /* unused */) override {}

  bool isKeyLoaded() const override {
    return false;
  }

  std::unique_ptr<folly::IOBuf> decrypt(
      folly::StringPiece /* unused */) const override {
    DWIO_RAISE("Failed to access encrypted data");
  }

  std::unique_ptr<Decrypter> clone() const override {
    return std::make_unique<DummyDecrypter>();
  }
};

// Fallback decypter factory used when file is encrypted and user doesn't
// supply one. This allows unencrypted columns to be read from encrypted file
// without factory
class DummyDecrypterFactory : public DecrypterFactory {
 public:
  std::unique_ptr<Decrypter> create(EncryptionProvider /* unused */) override {
    return std::make_unique<DummyDecrypter>();
  }
};

} // namespace encryption
} // namespace common
} // namespace dwio
} // namespace facebook
