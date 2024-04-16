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

#include "velox/dwio/dwrf/reader/SelectiveFlatMapColumnReader.h"

#include "velox/dwio/common/FlatMapHelper.h"
#include "velox/dwio/dwrf/reader/SelectiveDwrfReader.h"
#include "velox/dwio/dwrf/reader/SelectiveStructColumnReader.h"

namespace facebook::velox::dwrf {

namespace {

template <typename T>
dwio::common::flatmap::KeyValue<T> extractKey(const proto::KeyInfo& info) {
  return dwio::common::flatmap::KeyValue<T>(info.intkey());
}

template <>
inline dwio::common::flatmap::KeyValue<StringView> extractKey<StringView>(
    const proto::KeyInfo& info) {
  return dwio::common::flatmap::KeyValue<StringView>(
      StringView(info.byteskey()));
}

template <typename T>
std::string toString(const T& x) {
  if constexpr (std::is_same_v<T, StringView>) {
    return x;
  } else {
    return std::to_string(x);
  }
}

template <typename T>
dwio::common::flatmap::KeyPredicate<T> prepareKeyPredicate(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    StripeStreams& stripe) {
  auto& cs = stripe.getColumnSelector();
  const auto expr = cs.getNode(requestedType->id())->getNode().expression;
  return dwio::common::flatmap::prepareKeyPredicate<T>(expr);
}

// Represent a branch of a value node in a flat map.  Represent a keyed value
// node.
template <typename T>
struct KeyNode {
  dwio::common::flatmap::KeyValue<T> key;
  uint32_t sequence;
  std::unique_ptr<dwio::common::SelectiveColumnReader> reader;
  std::unique_ptr<BooleanRleDecoder> inMap;

  KeyNode(
      const dwio::common::flatmap::KeyValue<T>& key,
      uint32_t sequence,
      std::unique_ptr<dwio::common::SelectiveColumnReader> reader,
      std::unique_ptr<BooleanRleDecoder> inMap)
      : key(key),
        sequence(sequence),
        reader(std::move(reader)),
        inMap(std::move(inMap)) {}
};

template <typename T>
std::vector<KeyNode<T>> getKeyNodes(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    DwrfParams& params,
    common::ScanSpec& scanSpec,
    bool asStruct) {
  using namespace dwio::common::flatmap;

  std::vector<KeyNode<T>> keyNodes;
  std::unordered_set<size_t> processed;

  auto& requestedValueType = requestedType->childAt(1);
  auto& dataValueType = fileType->childAt(1);
  auto& stripe = params.stripeStreams();
  auto keyPredicate = prepareKeyPredicate<T>(requestedType, stripe);

  common::ScanSpec* keysSpec = nullptr;
  common::ScanSpec* valuesSpec = nullptr;
  if (!asStruct) {
    keysSpec = scanSpec.getOrCreateChild(
        common::Subfield(common::ScanSpec::kMapKeysFieldName));
    valuesSpec = scanSpec.getOrCreateChild(
        common::Subfield(common::ScanSpec::kMapValuesFieldName));
    VELOX_CHECK(!valuesSpec->hasFilter());
    keysSpec->setProjectOut(true);
    keysSpec->setExtractValues(true);
    valuesSpec->setProjectOut(true);
    valuesSpec->setExtractValues(true);
  }

  std::unordered_map<KeyValue<T>, common::ScanSpec*, KeyValueHash<T>>
      childSpecs;
  if (asStruct) {
    for (auto& c : scanSpec.children()) {
      if constexpr (std::is_same_v<T, StringView>) {
        childSpecs[KeyValue<T>(StringView(c->fieldName()))] = c.get();
      } else {
        childSpecs[KeyValue<T>(c->subscript())] = c.get();
      }
    }
  }

  // Load all sub streams.
  // Fetch reader, in map bitmap and key object.
  auto streams = stripe.visitStreamsOfNode(
      dataValueType->id(), [&](const StreamInformation& stream) {
        auto sequence = stream.getSequence();
        // No need to load shared dictionary stream here.
        if (sequence == 0 || processed.count(sequence)) {
          return;
        }
        processed.insert(sequence);
        EncodingKey seqEk(dataValueType->id(), sequence);
        const auto& keyInfo = stripe.getEncoding(seqEk).key();
        auto key = extractKey<T>(keyInfo);
        // Check if we have key filter passed through read schema.
        if (!keyPredicate(key)) {
          return;
        }
        common::ScanSpec* childSpec;
        if (auto it = childSpecs.find(key);
            it != childSpecs.end() && !it->second->isConstant()) {
          childSpec = it->second;
        } else if (asStruct) {
          // Column not selected in 'scanSpec', skipping it.
          return;
        } else {
          if (keysSpec && keysSpec->filter() &&
              !common::applyFilter(*keysSpec->filter(), key.get())) {
            return; // Subfield pruning
          }
          childSpecs[key] = childSpec = valuesSpec;
        }
        auto labels = params.streamLabels().append(toString(key.get()));
        auto inMap = stripe.getStream(
            seqEk.forKind(proto::Stream_Kind_IN_MAP), labels.label(), true);
        VELOX_CHECK(inMap, "In map stream is required");
        auto inMapDecoder = createBooleanRleDecoder(std::move(inMap), seqEk);
        DwrfParams childParams(
            stripe,
            labels,
            params.runtimeStatistics(),
            FlatMapContext{
                .sequence = sequence,
                .inMapDecoder = inMapDecoder.get(),
                .keySelectionCallback = nullptr});
        auto reader = SelectiveDwrfReader::build(
            requestedValueType, dataValueType, childParams, *childSpec);
        keyNodes.emplace_back(
            key, sequence, std::move(reader), std::move(inMapDecoder));
      });

  VLOG(1) << "[Flat-Map] Initialized a flat-map column reader for node "
          << fileType->id() << ", keys=" << keyNodes.size()
          << ", streams=" << streams;

  return keyNodes;
}

template <typename T>
class SelectiveFlatMapAsStructReader : public SelectiveStructColumnReaderBase {
 public:
  SelectiveFlatMapAsStructReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec,
      const std::vector<std::string>& /*keys*/)
      : SelectiveStructColumnReaderBase(
            requestedType,
            fileType,
            params,
            scanSpec),
        keyNodes_(
            getKeyNodes<T>(requestedType, fileType, params, scanSpec, true)) {
    VELOX_CHECK(
        !keyNodes_.empty(),
        "For struct encoding, keys to project must be configured");
    children_.resize(keyNodes_.size());
    for (int i = 0; i < keyNodes_.size(); ++i) {
      keyNodes_[i].reader->scanSpec()->setSubscript(i);
      children_[i] = keyNodes_[i].reader.get();
    }
  }

 private:
  std::vector<KeyNode<T>> keyNodes_;
};

template <typename T>
class SelectiveFlatMapReader : public SelectiveStructColumnReaderBase {
 public:
  SelectiveFlatMapReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec)
      : SelectiveStructColumnReaderBase(
            requestedType,
            fileType,
            params,
            scanSpec),
        keyNodes_(
            getKeyNodes<T>(requestedType, fileType, params, scanSpec, false)) {
    std::sort(keyNodes_.begin(), keyNodes_.end(), [](auto& x, auto& y) {
      return x.sequence < y.sequence;
    });
    children_.resize(keyNodes_.size());
    for (int i = 0; i < keyNodes_.size(); ++i) {
      children_[i] = keyNodes_[i].reader.get();
      children_[i]->setIsFlatMapValue(true);
    }
    if (auto type = requestedType_->type()->childAt(1); type->isRow()) {
      childValues_ = BaseVector::create(type, 0, &memoryPool_);
    }
  }

  bool useBulkPath() const override {
    return false;
  }

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override {
    numReads_ = scanSpec_->newRead();
    prepareRead<char>(offset, rows, incomingNulls);
    VELOX_DCHECK(!hasMutation());
    auto activeRows = rows;
    auto* mapNulls =
        nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
    if (scanSpec_->filter()) {
      auto kind = scanSpec_->filter()->kind();
      VELOX_CHECK(
          kind == common::FilterKind::kIsNull ||
          kind == common::FilterKind::kIsNotNull);
      filterNulls<int32_t>(rows, kind == common::FilterKind::kIsNull, false);
      if (outputRows_.empty()) {
        for (auto* reader : children_) {
          reader->addParentNulls(offset, mapNulls, rows);
        }
        return;
      }
      activeRows = outputRows_;
    }
    // Separate the loop to be cache friendly.
    for (auto* reader : children_) {
      advanceFieldReader(reader, offset);
    }
    for (auto* reader : children_) {
      reader->read(offset, activeRows, mapNulls);
      reader->addParentNulls(offset, mapNulls, rows);
    }
    lazyVectorReadOffset_ = offset;
    readOffset_ = offset + rows.back() + 1;
  }

  void getValues(RowSet rows, VectorPtr* result) override {
    auto& mapResult = prepareResult(*result, rows.size());
    auto* rawOffsets = mapResult.mutableOffsets(rows.size())
                           ->template asMutable<vector_size_t>();
    auto* rawSizes = mapResult.mutableSizes(rows.size())
                         ->template asMutable<vector_size_t>();
    auto numNestedRows = calculateOffsets(rows, rawOffsets, rawSizes);
    auto& keys = mapResult.mapKeys();
    auto& values = mapResult.mapValues();
    BaseVector::prepareForReuse(keys, numNestedRows);
    BaseVector::prepareForReuse(values, numNestedRows);
    auto* flatKeys = keys->template asFlatVector<T>();
    VELOX_DYNAMIC_TYPE_DISPATCH(
        copyValues, values->typeKind(), rows, flatKeys, rawOffsets, *values);
    VELOX_CHECK_EQ(rawOffsets[rows.size() - 1], numNestedRows);
    std::copy_backward(
        rawOffsets, rawOffsets + rows.size() - 1, rawOffsets + rows.size());
    rawOffsets[0] = 0;
    result->get()->setNulls(resultNulls());
  }

 private:
  MapVector& prepareResult(VectorPtr& result, vector_size_t size) {
    if (result && result->encoding() == VectorEncoding::Simple::MAP &&
        result.unique()) {
      result->resetDataDependentFlags(nullptr);
      result->resize(size);
    } else {
      VLOG(1) << "Reallocating result MAP vector of size " << size;
      result = BaseVector::create(requestedType_->type(), size, &memoryPool_);
    }
    return *result->asUnchecked<MapVector>();
  }

  vector_size_t
  calculateOffsets(RowSet rows, vector_size_t* offsets, vector_size_t* sizes) {
    auto* nulls =
        nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
    inMaps_.resize(children_.size());
    for (int k = 0; k < children_.size(); ++k) {
      auto& data = static_cast<const DwrfData&>(children_[k]->formatData());
      inMaps_[k] = data.inMap();
      if (!inMaps_[k]) {
        inMaps_[k] = nulls;
      }
    }
    columnRowBits_.resize(bits::nwords(children_.size() * rows.size()));
    std::fill(columnRowBits_.begin(), columnRowBits_.end(), 0);
    std::fill(sizes, sizes + rows.size(), 0);
    for (int k = 0; k < children_.size(); ++k) {
      if (inMaps_[k]) {
        for (vector_size_t i = 0; i < rows.size(); ++i) {
          if (bits::isBitSet(inMaps_[k], rows[i])) {
            bits::setBit(columnRowBits_.data(), i + k * rows.size());
            ++sizes[i];
          }
        }
      } else {
        bits::fillBits(
            columnRowBits_.data(),
            k * rows.size(),
            (k + 1) * rows.size(),
            true);
        for (vector_size_t i = 0; i < rows.size(); ++i) {
          ++sizes[i];
        }
      }
    }
    vector_size_t numNestedRows = 0;
    for (vector_size_t i = 0; i < rows.size(); ++i) {
      if (nulls && bits::isBitNull(nulls, rows[i])) {
        if (!returnReaderNulls_) {
          bits::setNull(rawResultNulls_, i);
        }
        anyNulls_ = true;
      }
      offsets[i] = numNestedRows;
      numNestedRows += sizes[i];
    }
    return numNestedRows;
  }

  template <TypeKind kKind>
  void copyValues(
      RowSet rows,
      FlatVector<T>* flatKeys,
      vector_size_t* rawOffsets,
      BaseVector& values) {
    // String values are not copied directly because currently we don't have
    // them in production so no need to optimize.
    constexpr bool kDirectCopy =
        TypeKind::TINYINT <= kKind && kKind <= TypeKind::DOUBLE;
    using ValueType = typename TypeTraits<kKind>::NativeType;
    T* rawKeys = flatKeys->mutableRawValues();
    [[maybe_unused]] size_t strKeySize;
    [[maybe_unused]] char* rawStrKeyBuffer;
    if constexpr (std::is_same_v<T, StringView>) {
      strKeySize = 0;
      for (int k = 0; k < children_.size(); ++k) {
        if (!keyNodes_[k].key.get().isInline()) {
          strKeySize += keyNodes_[k].key.get().size();
        }
      }
      if (strKeySize > 0) {
        auto buf = AlignedBuffer::allocate<char>(strKeySize, &memoryPool_);
        rawStrKeyBuffer = buf->template asMutable<char>();
        flatKeys->addStringBuffer(buf);
        strKeySize = 0;
        for (int k = 0; k < children_.size(); ++k) {
          auto& s = keyNodes_[k].key.get();
          if (!s.isInline()) {
            memcpy(&rawStrKeyBuffer[strKeySize], s.data(), s.size());
            strKeySize += s.size();
          }
        }
        strKeySize = 0;
      }
    }
    [[maybe_unused]] ValueType* targetValues;
    [[maybe_unused]] uint64_t* targetNulls;
    if constexpr (kDirectCopy) {
      VELOX_CHECK(values.isFlatEncoding());
      auto* flat = values.asUnchecked<FlatVector<ValueType>>();
      targetValues = flat->mutableRawValues();
      targetNulls = flat->mutableRawNulls();
    }
    for (int k = 0; k < children_.size(); ++k) {
      [[maybe_unused]] StringView strKey;
      if constexpr (std::is_same_v<T, StringView>) {
        strKey = keyNodes_[k].key.get();
        if (!strKey.isInline()) {
          strKey = {
              &rawStrKeyBuffer[strKeySize],
              static_cast<int32_t>(strKey.size())};
          strKeySize += strKey.size();
        }
      }
      children_[k]->getValues(rows, &childValues_);
      if constexpr (kDirectCopy) {
        decodedChildValues_.decode(*childValues_);
      }
      const auto begin = k * rows.size();
      bits::forEachSetBit(
          columnRowBits_.data(),
          begin,
          begin + rows.size(),
          [&](vector_size_t i) {
            i -= begin;
            if constexpr (std::is_same_v<T, StringView>) {
              rawKeys[rawOffsets[i]] = strKey;
            } else {
              rawKeys[rawOffsets[i]] = keyNodes_[k].key.get();
            }
            if constexpr (kDirectCopy) {
              targetValues[rawOffsets[i]] =
                  decodedChildValues_.valueAt<ValueType>(i);
              bits::setNull(
                  targetNulls, rawOffsets[i], decodedChildValues_.isNullAt(i));
            } else {
              copyRanges_.push_back({
                  .sourceIndex = i,
                  .targetIndex = rawOffsets[i],
                  .count = 1,
              });
            }
            ++rawOffsets[i];
          });
      if constexpr (!kDirectCopy) {
        values.copyRanges(childValues_.get(), copyRanges_);
        copyRanges_.clear();
      }
    }
  }

  std::vector<KeyNode<T>> keyNodes_;
  VectorPtr childValues_;
  DecodedVector decodedChildValues_;
  std::vector<const uint64_t*> inMaps_;
  std::vector<uint64_t> columnRowBits_;
  std::vector<BaseVector::CopyRange> copyRanges_;
};

template <typename T>
std::unique_ptr<dwio::common::SelectiveColumnReader> createReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    DwrfParams& params,
    common::ScanSpec& scanSpec) {
  auto& mapColumnIdAsStruct =
      params.stripeStreams().getRowReaderOptions().getMapColumnIdAsStruct();
  auto it = mapColumnIdAsStruct.find(requestedType->id());
  if (it != mapColumnIdAsStruct.end()) {
    return std::make_unique<SelectiveFlatMapAsStructReader<T>>(
        requestedType, fileType, params, scanSpec, it->second);
  } else {
    return std::make_unique<SelectiveFlatMapReader<T>>(
        requestedType, fileType, params, scanSpec);
  }
}

} // namespace

std::unique_ptr<dwio::common::SelectiveColumnReader>
createSelectiveFlatMapColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    DwrfParams& params,
    common::ScanSpec& scanSpec) {
  auto kind = fileType->childAt(0)->type()->kind();
  switch (kind) {
    case TypeKind::TINYINT:
      return createReader<int8_t>(requestedType, fileType, params, scanSpec);
    case TypeKind::SMALLINT:
      return createReader<int16_t>(requestedType, fileType, params, scanSpec);
    case TypeKind::INTEGER:
      return createReader<int32_t>(requestedType, fileType, params, scanSpec);
    case TypeKind::BIGINT:
      return createReader<int64_t>(requestedType, fileType, params, scanSpec);
    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR:
      return createReader<StringView>(
          requestedType, fileType, params, scanSpec);
    default:
      VELOX_UNSUPPORTED("Not supported key type: {}", kind);
  }
}

} // namespace facebook::velox::dwrf
