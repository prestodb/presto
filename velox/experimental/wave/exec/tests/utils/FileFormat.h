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

#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/type/StringView.h"
#include "velox/vector/ComplexVector.h"

/// Sample set of composable encodings. Bit packing, direct and dictionary.
namespace facebook::velox::wave::test {

class Table;

enum Encoding { kFlat, kDict };

struct Column {
  TypeKind kind;
  Encoding encoding;
  // Number of encoded values.
  int32_t numValues{0};

  // Distinct values in kDict.
  std::shared_ptr<Column> alphabet;

  // Encoded lengths for strings in 'data' if type is varchar and encoding is
  // kFlat.
  std::unique_ptr<Column> lengths;

  // Width of bit packed encoding.
  int8_t bitWidth;

  BufferPtr values;

  /// Frame of reference base for kFlat.
  int64_t baseline{0};

  /// Encoded column with 'numValues' null bits, nullptr if no nulls. If set,
  /// 'values' has an entry for each non-null.
  std::unique_ptr<Column> nulls;
};

struct Stripe {
  Stripe(
      std::vector<std::unique_ptr<Column>>&& in,
      const std::shared_ptr<const dwio::common::TypeWithId>& type)
      : typeWithId(type), columns(std::move(in)) {}

  const Column* findColumn(const dwio::common::TypeWithId& child) const;

  // Unique name assigned when associating with a Table.
  std::string name;

  std::shared_ptr<const dwio::common::TypeWithId> typeWithId;

  // Top level columns.
  std::vector<std::unique_ptr<Column>> columns;
};

class StringSet {
 public:
  StringSet(memory::MemoryPool* pool) : pool_(pool) {}

  StringView add(StringView data);
  std::unique_ptr<Column> toColumn();

 private:
  int64_t totalSize_{0};
  int32_t maxLength_{0};
  std::vector<int32_t> lengths_;
  std::vector<BufferPtr> buffers_;
  memory::MemoryPool* pool_;
};

class EncoderBase {
 public:
  virtual ~EncoderBase() = default;

  virtual void append(const VectorPtr& data) = 0;

  virtual std::unique_ptr<Column> toColumn() = 0;
};

template <typename T>
class Encoder : public EncoderBase {
 public:
  Encoder(memory::MemoryPool* pool, const TypePtr& type)
      : pool_(pool),
        kind_(type->kind()),
        dictStrings_(pool),
        allStrings_(pool) {}

  // Adds data.
  void append(const VectorPtr& data) override;

  // Retrieves the data added so far as an encoded column.
  std::unique_ptr<Column> toColumn() override;

 private:
  void appendTyped(VectorPtr data);

  void add(T data);

  void addNull();

  int64_t flatSize();
  int64_t dictSize();

  memory::MemoryPool* pool_;
  TypeKind kind_;
  int32_t count_{0};
  int32_t nonNullCount_{0};
  // Distincts for either int64_t or double.
  folly::F14FastMap<T, int32_t> distincts_;
  // Values as indices into dicts.
  std::vector<int32_t> indices_;

  std::vector<T> dictInts_;
  // The fixed width values as direct.
  std::vector<T> direct_;
  // True if too many distinct values for dict.
  bool abandonDict_{false};
  T max_{};
  T min_{};
  // longest string, if string type.
  int32_t maxLength_{0};
  // Total bytes in distinct strings.
  int64_t dictBytes_{0};
  // Total string bytes without dict.
  int32_t totalStringBytes_{0};

  StringSet dictStrings_;
  StringSet allStrings_;
  // If nulls occur, has a 1 bit for each non-null. Empty if no nulls.
  std::vector<uint64_t> nulls_;
};

template <>
void Encoder<StringView>::add(StringView data);

template <>
int64_t Encoder<StringView>::dictSize();

template <>
int64_t Encoder<StringView>::flatSize();

template <>
int64_t Encoder<Timestamp>::flatSize();
template <>
int64_t Encoder<double>::flatSize();
template <>
int64_t Encoder<float>::flatSize();

class Writer {
 public:
  Writer(int32_t stripeSize)
      : stripeSize_(stripeSize),
        pool_(memory::MemoryManager::getInstance()->addLeafPool()) {}

  /// Appends a batch of data.
  void append(RowVectorPtr data);

  // Finishes encoding data, makes the table ready to read.
  Table* finalize(std::string tableName);

 private:
  TypePtr type_;
  void finishStripe();
  const int32_t stripeSize_;
  std::vector<std::unique_ptr<Stripe>> stripes_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::vector<std::unique_ptr<EncoderBase>> encoders_;
};

using SplitVector = std::vector<std::shared_ptr<connector::ConnectorSplit>>;

class Table {
 public:
  Table(const std::string name) : name_(name) {}

  static const Table* defineTable(
      const std::string& name,
      const std::vector<RowVectorPtr>& data);

  static Table* getTable(const std::string& name, bool makeNew = false) {
    std::lock_guard<std::mutex> l(mutex_);
    auto it = allTables_.find(name);
    if (it == allTables_.end()) {
      if (makeNew) {
        auto table = std::make_unique<Table>(name);
        auto ptr = table.get();
        allTables_[name] = std::move(table);
        return ptr;
      }
      return nullptr;
    }
    return it->second.get();
  }

  static void dropTable(const std::string& name);

  static Stripe* getStripe(const std::string& path) {
    std::lock_guard<std::mutex> l(mutex_);
    auto it = allStripes_.find(path);
    VELOX_CHECK(it != allStripes_.end());
    return it->second;
  }

  void addStripes(
      std::vector<std::unique_ptr<Stripe>>&& stripes,
      std::shared_ptr<memory::MemoryPool> pool);

  int32_t numStripes() const {
    return stripes_.size();
  }

  Stripe* stripeAt(int32_t index) {
    std::lock_guard<std::mutex> l(mutex_);
    if (index >= stripes_.size()) {
      return nullptr;
    }
    return stripes_[index].get();
  }

  SplitVector splits() const {
    SplitVector result;
    std::lock_guard<std::mutex> l(mutex_);
    for (auto& stripe : stripes_) {
      result.push_back(std::make_shared<connector::hive::HiveConnectorSplit>(
          "test-hive", stripe->name, dwio::common::FileFormat::UNKNOWN));
    }
    return result;
  }

  /// Drops all tables and stripes. Do this at end of test for cleanup.
  static void dropAll();

 private:
  static std::mutex mutex_;
  static std::unordered_map<std::string, std::unique_ptr<Table>> allTables_;
  static std::unordered_map<std::string, Stripe*> allStripes_;
  std::vector<std::shared_ptr<memory::MemoryPool>> pools_;
  std::string name_;
  std::vector<std::unique_ptr<Stripe>> stripes_;
};

} // namespace facebook::velox::wave::test
