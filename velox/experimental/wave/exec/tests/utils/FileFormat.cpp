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

#include "velox/experimental/wave/exec/tests/utils/FileFormat.h"
#include <iostream>

namespace facebook::velox::wave::test {

const Column* Stripe::findColumn(const dwio::common::TypeWithId& child) const {
  for (auto i = 0; i < typeWithId->size(); ++i) {
    if (typeWithId->childAt(i)->id() == child.id()) {
      return columns[i].get();
    }
  }
  VELOX_FAIL("No such column {}", child.id());
}

std::mutex Table::mutex_;
std::unordered_map<std::string, std::unique_ptr<Table>> Table::allTables_;
std::unordered_map<std::string, Stripe*> Table::allStripes_;

int32_t bitWidth(uint64_t max) {
  return 64 - __builtin_clzll(max);
}

template <typename T>
T subtractMin(T value, T min) {
  return value - min;
}

template <>
StringView subtractMin(StringView value, StringView /*min*/) {
  return value;
}

template <>
Timestamp subtractMin(Timestamp value, Timestamp /*min*/) {
  return value;
}

template <>
double subtractMin(double value, double /*min*/) {
  return value;
}

template <>
float subtractMin(float value, float /*min*/) {
  return value;
}

template <typename T>
int64_t baseValue(T value) {
  return static_cast<int64_t>(value);
}

template <>
int64_t baseValue(StringView value) {
  return 0;
}

template <>
int64_t baseValue(Timestamp value) {
  return 0;
}

template <>
int64_t baseValue(double value) {
  return 0;
}
template <>
int64_t baseValue(float value) {
  return 0;
}

template <typename T>
int32_t rangeBitWidth(T max, T min) {
  auto bits = bitWidth(baseValue(subtractMin(max, min)));
  return bits ? bits : sizeof(T) * 8;
}

template <typename T>
BufferPtr
encodeInts(const std::vector<T>& ints, T min, T max, memory::MemoryPool* pool) {
  int32_t width = rangeBitWidth(max, min);
  int32_t size = bits::roundUp(ints.size() * width, 128) / 8;
  auto buffer = AlignedBuffer::allocate<char>(size, pool);
  auto destination = buffer->asMutable<uint64_t>();
  for (auto i = 0; i < ints.size(); ++i) {
    T sourceValue = subtractMin(ints[i], min);
    bits::copyBits(
        reinterpret_cast<uint64_t*>(&sourceValue),
        0,
        destination,
        i * width,
        width);
  }
  return buffer;
}

template <TypeKind kind>
std::unique_ptr<EncoderBase> makeTypedEncoder(
    memory::MemoryPool& pool,
    const TypePtr& type) {
  using T = typename TypeTraits<kind>::NativeType;
  return std::make_unique<Encoder<T>>(&pool, type);
}

std::unique_ptr<EncoderBase> makeEncoder(
    memory::MemoryPool& pool,
    const TypePtr& type) {
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      makeTypedEncoder, type->kind(), pool, type);
}

template <typename T>
int64_t Encoder<T>::flatSize() {
  return count_ * rangeBitWidth(max_, min_) / 8;
}

template <>
int64_t Encoder<StringView>::flatSize() {
  return totalStringBytes_ + (count_ * bitWidth(maxLength_) / 8);
}

template <>
int64_t Encoder<Timestamp>::flatSize() {
  return direct_.size() * sizeof(Timestamp);
}

template <>
int64_t Encoder<double>::flatSize() {
  return direct_.size() * sizeof(double);
}

template <>
int64_t Encoder<float>::flatSize() {
  return direct_.size() * sizeof(float);
}

template <typename T>
int64_t Encoder<T>::dictSize() {
  return (rangeBitWidth(max_, min_) * distincts_.size() / 8) +
      (bitWidth(distincts_.size() - 1) * count_ / 8);
}

template <>
int64_t Encoder<StringView>::dictSize() {
  return (count_ * bitWidth(distincts_.size() - 1) / 8) + dictBytes_;
}

struct StringWithId {
  StringView string;
  int32_t id;
};

void printSums(uint64_t* bits, int32_t numBits) {
  std::cout << "***Flags\n";
  int32_t cnt = 0;
  for (auto i = 0; i < bits::nwords(numBits) - 16; i += 16) {
    cnt += bits::countBits(bits, i * 64, (i + 16) * 64);
    std::cout << fmt::format("{}: {}\n", (i + 16) * 64, cnt);
  }
}

std::unique_ptr<Column>
encodeBits(uint64_t* bits, int32_t numBits, memory::MemoryPool* pool) {
  auto column = std::make_unique<Column>();
  column->encoding = kFlat;
  column->kind = TypeKind::BOOLEAN;
  column->numValues = numBits;
  column->values = AlignedBuffer::allocate<bool>(numBits, pool);
  memcpy(column->values->asMutable<char>(), bits, bits::nbytes(numBits));
  column->bitWidth = 1;
  // printSums(bits, numBits);
  return column;
}

template <typename T>
std::unique_ptr<Column>
directInts(std::vector<T>& ints, T min, T max, memory::MemoryPool* pool) {
  auto column = std::make_unique<Column>();
  column->values = encodeInts(ints, min, max, pool);
  column->numValues = ints.size();
  return column;
}

template <typename T>
std::unique_ptr<Column> Encoder<T>::toColumn() {
  auto column = std::make_unique<Column>();
  column->kind = kind_;
  if (!nulls_.empty()) {
    column->nulls = encodeBits(nulls_.data(), count_, pool_);
  }
  if (distincts_.size() <= 1) {
    VELOX_NYI("constant not supported");
  }
  if (!abandonDict_ && dictSize() < flatSize()) {
    if (kind_ == TypeKind::VARCHAR) {
      column->encoding = kDict;
      column->values = encodeInts(
          indices_, 0, static_cast<int32_t>(distincts_.size() - 1), pool_);
      column->bitWidth = bitWidth(distincts_.size() - 1);
      column->alphabet = dictStrings_.toColumn();
      return column;
    } else {
      column->alphabet = directInts(dictInts_, min_, max_, pool_);
    }
  }
  column->values = encodeInts(direct_, min_, max_, pool_);
  column->numValues = count_;
  column->bitWidth = rangeBitWidth(max_, min_);
  column->baseline = baseValue(min_);
  return column;
}

template <typename T>
void Encoder<T>::add(T data) {
  if (direct_.empty()) {
    min_ = data;
    max_ = data;
  } else {
    if (data > max_) {
      max_ = data;
    } else if (data < min_) {
      min_ = data;
    }
  }
  ++count_;
  ++nonNullCount_;
  direct_.push_back(data);
  if (abandonDict_) {
    return;
  }
  auto it = distincts_.find(data);
  if (it != distincts_.end()) {
    indices_.push_back(it->second);
    return;
  }
  auto id = distincts_.size();
  distincts_[data] = id;
  dictInts_.push_back(data);
  indices_.push_back(id);
}

StringView StringSet::add(StringView data) {
  int32_t stringSize = data.size();
  totalSize_ += stringSize;
  if (stringSize > maxLength_) {
    maxLength_ = stringSize;
  }
  if (buffers_.empty() ||
      buffers_.back()->size() + stringSize > buffers_.back()->capacity()) {
    buffers_.push_back(AlignedBuffer::allocate<char>(1 << 20, pool_));
    buffers_.back()->setSize(0);
  }
  lengths_.push_back(stringSize);
  auto& buffer = buffers_.back();
  auto size = buffer->size();
  memcpy(buffer->asMutable<char>() + size, data.data(), data.size());
  buffer->setSize(size + data.size());
  return StringView(buffer->as<char>() + size, data.size());
};

std::unique_ptr<Column> StringSet::toColumn() {
  auto buffer = AlignedBuffer::allocate<char>(totalSize_, pool_);
  for (auto& piece : buffers_) {
    memcpy(buffer->asMutable<char>(), piece->as<char>(), piece->size());
  }
  auto column = std::make_unique<Column>();
  column->kind = TypeKind::VARCHAR;
  column->encoding = kFlat;
  column->values = buffer;
  column->lengths = directInts(lengths_, 0, maxLength_, pool_);
  column->bitWidth = bitWidth(maxLength_);
  return column;
}

template <>
void Encoder<StringView>::add(StringView data) {
  ++count_;
  ++nonNullCount_;
  auto size = data.size();
  totalStringBytes_ += size;
  if (size > maxLength_) {
    maxLength_ = size;
  }

  if (abandonDict_) {
    allStrings_.add(data);
    return;
  }
  auto it = distincts_.find(data);
  if (it != distincts_.end()) {
    indices_.push_back(it->second);
    return;
  }
  dictBytes_ += data.size();
  auto copy = dictStrings_.add(data);
  auto id = distincts_.size();
  distincts_[copy] = id;
  indices_.push_back(id);
}

template <typename T>
void Encoder<T>::addNull() {
  ++count_;
  auto n = bits::nwords(count_);
  if (nulls_.size() < n) {
    nulls_.resize(n, bits::kNotNull64);
  }
  bits::setBit(nulls_.data(), count_ - 1, bits::kNull);
}

template <typename T>
void Encoder<T>::append(const VectorPtr& data) {
  auto size = data->size();
  SelectivityVector allRows(size);
  DecodedVector decoded(*data, allRows, true);
  for (auto i = 0; i < size; ++i) {
    if (decoded.isNullAt(i)) {
      addNull();
    } else {
      add(decoded.valueAt<T>(i));
    }
  }
}

void Writer::append(RowVectorPtr data) {
  type_ = data->type();
  if (encoders_.empty()) {
    for (auto i = 0; i < data->type()->size(); ++i) {
      encoders_.push_back(
          makeEncoder(*pool_, data->type()->as<TypeKind::ROW>().childAt(i)));
    }
  }
  VELOX_CHECK_EQ(encoders_.size(), data->type()->size());
  for (auto i = 0; i < encoders_.size(); ++i) {
    encoders_[i]->append(data->childAt(i));
  }
  rowsInStripe_ += data->size();
  if (rowsInStripe_ >= stripeSize_) {
    finishStripe();
  }
}

void Writer::finishStripe() {
  if (encoders_.empty()) {
    return;
  }
  std::vector<std::unique_ptr<Column>> columns;
  for (auto& encoder : encoders_) {
    columns.push_back(encoder->toColumn());
  }
  stripes_.push_back(std::make_unique<Stripe>(
      std::move(columns), dwio::common::TypeWithId::create(type_)));
  encoders_.clear();
  rowsInStripe_ = 0;
}

Table* Writer::finalize(std::string tableName) {
  finishStripe();
  auto table = Table::getTable(tableName, true);
  table->addStripes(std::move(stripes_), pool_);
  return table;
}

void Table::addStripes(
    std::vector<std::unique_ptr<Stripe>>&& stripes,
    std::shared_ptr<memory::MemoryPool> pool) {
  std::lock_guard<std::mutex> l(mutex_);
  for (auto& s : stripes) {
    s->name = fmt::format("wavemock://{}/{}", name_, stripes_.size());
    allStripes_[s->name] = s.get();
    stripes_.push_back(std::move(s));
  }
  pools_.push_back(pool);
}

// static
const Table* Table::defineTable(
    const std::string& name,
    const std::vector<RowVectorPtr>& data) {
  dropTable(name);
  Writer writer(data[0]->size());
  for (auto& vector : data) {
    writer.append(vector);
  }
  return writer.finalize(name);
}

//  static
void Table::dropTable(const std::string& name) {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = allTables_.find(name);
  if (it == allTables_.end()) {
    return;
  }
  auto& table = it->second;
  for (auto& stripe : table->stripes_) {
    allStripes_.erase(stripe->name);
  }
  allTables_.erase(it);
}
// static
void Table::dropAll() {
  std::lock_guard<std::mutex> l(mutex_);
  allStripes_.clear();
  allTables_.clear();
}

} // namespace facebook::velox::wave::test
