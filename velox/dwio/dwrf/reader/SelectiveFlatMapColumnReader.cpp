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

bool noFastPath = false;

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

  std::shared_ptr<common::ScanSpec> keysSpec;
  std::shared_ptr<common::ScanSpec> valuesSpec;
  if (!asStruct) {
    if (auto keys = scanSpec.childByName(common::ScanSpec::kMapKeysFieldName)) {
      keysSpec = scanSpec.removeChild(keys);
    }
    if (auto values =
            scanSpec.childByName(common::ScanSpec::kMapValuesFieldName)) {
      VELOX_CHECK(!values->hasFilter());
      valuesSpec = scanSpec.removeChild(values);
    }
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
          childSpec =
              scanSpec.getOrCreateChild(common::Subfield(toString(key.get())));
          childSpec->setProjectOut(true);
          childSpec->setExtractValues(true);
          if (valuesSpec) {
            *childSpec = *valuesSpec;
          }
          childSpecs[key] = childSpec;
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
        // Copy the scan spec because we need to remove the children.
        structScanSpec_(scanSpec) {
    scanSpec_ = &structScanSpec_;
    keyNodes_ =
        getKeyNodes<T>(requestedType, fileType, params, structScanSpec_, false);
    std::sort(keyNodes_.begin(), keyNodes_.end(), [](auto& x, auto& y) {
      return x.sequence < y.sequence;
    });
    childValues_.resize(keyNodes_.size());
    copyRanges_.resize(keyNodes_.size());
    children_.resize(keyNodes_.size());
    for (int i = 0; i < keyNodes_.size(); ++i) {
      children_[i] = keyNodes_[i].reader.get();
    }
    if (auto type = requestedType_->type()->childAt(1); type->isRow()) {
      for (auto& vec : childValues_) {
        vec = BaseVector::create(type, 0, &memoryPool_);
      }
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
    for (auto* reader : children_) {
      advanceFieldReader(reader, offset);
      reader->read(offset, activeRows, mapNulls);
      reader->addParentNulls(offset, mapNulls, rows);
    }
    lazyVectorReadOffset_ = offset;
    readOffset_ = offset + rows.back() + 1;
  }

  void getValues(RowSet rows, VectorPtr* result) override {
    for (int k = 0; k < children_.size(); ++k) {
      children_[k]->getValues(rows, &childValues_[k]);
      copyRanges_[k].clear();
    }
    auto offsets =
        AlignedBuffer::allocate<vector_size_t>(rows.size(), &memoryPool_);
    auto sizes =
        AlignedBuffer::allocate<vector_size_t>(rows.size(), &memoryPool_);
    auto* nulls =
        nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;

    if (!noFastPath && fastPath(offsets, sizes, rows, nulls, result)) {
      return;
    }
    auto* rawOffsets = offsets->template asMutable<vector_size_t>();
    auto* rawSizes = sizes->template asMutable<vector_size_t>();
    vector_size_t totalSize = 0;
    for (vector_size_t i = 0; i < rows.size(); ++i) {
      if (nulls && bits::isBitNull(nulls, rows[i])) {
        rawSizes[i] = 0;

        if (!returnReaderNulls_) {
          bits::setNull(rawResultNulls_, i);
        }

        anyNulls_ = true;

        continue;
      }
      int currentRowSize = 0;
      for (int k = 0; k < children_.size(); ++k) {
        auto& data = static_cast<const DwrfData&>(children_[k]->formatData());
        auto* inMap = data.inMap();
        if (inMap && bits::isBitNull(inMap, rows[i])) {
          continue;
        }
        copyRanges_[k].push_back({
            .sourceIndex = i,
            .targetIndex = totalSize + currentRowSize,
            .count = 1,
        });
        ++currentRowSize;
      }
      rawOffsets[i] = totalSize;
      rawSizes[i] = currentRowSize;
      totalSize += currentRowSize;
    }
    auto& mapType = requestedType_->type()->asMap();
    VectorPtr keys =
        BaseVector::create(mapType.keyType(), totalSize, &memoryPool_);
    VectorPtr values =
        BaseVector::create(mapType.valueType(), totalSize, &memoryPool_);
    auto* flatKeys = keys->asFlatVector<T>();
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
      for (auto& r : copyRanges_[k]) {
        if constexpr (std::is_same_v<T, StringView>) {
          rawKeys[r.targetIndex] = strKey;
        } else {
          rawKeys[r.targetIndex] = keyNodes_[k].key.get();
        }
      }
      values->copyRanges(childValues_[k].get(), copyRanges_[k]);
    }
    *result = std::make_shared<MapVector>(
        &memoryPool_,
        requestedType_->type(),
        resultNulls(),
        rows.size(),
        std::move(offsets),
        std::move(sizes),
        std::move(keys),
        std::move(values));
  }

 private:
  // Sets the bits for present and selected positions in 'rowColumnBits_' for
  // 'columnIdx' for the 64 rows selected by the inMap and selected bitmaps.
  // 'baseRow' is the number of selectd rows below the range covered by 'inMap'
  // and 'selected'
  void setRowBits(
      uint64_t inMap,
      uint64_t selected,
      int32_t baseRow,
      int32_t columnIdx) {
    auto pitch = children_.size();
    auto rowColumns = rowColumnBits_.data();
    auto selectedPresent = selected & inMap;
    while (selectedPresent) {
      int32_t row = __builtin_ctzll(selectedPresent);
      auto nthRow = __builtin_popcountll(selected & bits::lowMask(row));
      bits::setBit(rowColumns, (nthRow + baseRow) * pitch + columnIdx, true);
      selectedPresent &= selectedPresent - 1;
    }
  }

  // Returns the count of selected rows that have a value in inMap. Sets the
  // corresponding bits in 'rowColumnBits_'.
  int32_t countInMap(
      const uint64_t* inMap,
      const uint64_t* selected,
      int32_t numRows,
      int32_t columnIdx) {
    int32_t numSelected = 0;
    int32_t count = 0;
    bits::forEachWord(
        0,
        numRows,
        [&](int32_t idx, uint64_t mask) {
          count += __builtin_popcountll(inMap[idx] & selected[idx] & mask);
          setRowBits(inMap[idx], selected[idx] & mask, numSelected, columnIdx);
          numSelected += __builtin_popcountll(selected[idx] & mask);
        },
        [&](int32_t idx) {
          count += __builtin_popcountll(inMap[idx] & selected[idx]);
          setRowBits(inMap[idx], selected[idx], numSelected, columnIdx);
          numSelected += __builtin_popcountll(selected[idx]);
        });
    return count;
  }

  template <TypeKind kind>
  void fillValues(
      const BufferPtr& offsets,
      const BufferPtr& sizes,
      const uint64_t* mapNulls,
      RowSet rows,
      int32_t totalChildValues,
      T* rawKeys) {
    using V = typename TypeTraits<kind>::NativeType;
    auto rawOffsets = offsets->asMutable<int32_t>();
    auto rawSizes = sizes->asMutable<int32_t>();
    int32_t pitch = children_.size();
    auto values = AlignedBuffer::allocate<V>(totalChildValues, &memoryPool_);
    auto rawValues = values->template asMutable<V>();
    BufferPtr valueNulls;
    uint64_t* valueRawNulls = nullptr;
    if (nullsInChildValues_) {
      valueNulls = AlignedBuffer::allocate<bool>(
          totalChildValues, &memoryPool_, bits::kNotNull);
      valueRawNulls = valueNulls->asMutable<uint64_t>();
    }
    bool mayHaveDict = !childIndices_.empty();
    int32_t startBit = 0;
    int32_t fill = 0;
    for (int32_t rowIndex = 0; rowIndex < rows.size(); ++rowIndex) {
      int32_t row = rows[rowIndex];
      if (mapNulls && bits::isBitNull(mapNulls, row)) {
        rawSizes[rowIndex] = 0;
        rawOffsets[rowIndex] = 0;
        if (!returnReaderNulls_) {
          if (!rawResultNulls_) {
            mutableNulls(rows.size());
          }
          bits::setNull(rawResultNulls_, rowIndex);
          anyNulls_ = true;
        }
        // A row which is null for the whole map will only have not presents in
        // inMap.
        startBit += pitch;
        continue;
      }
      rawOffsets[rowIndex] = fill;
      bits::forEachSetBit(
          rowColumnBits_.data(),
          startBit,
          startBit + pitch,
          [&](int32_t index) {
            auto column = index - startBit;
            rawKeys[fill] = keyValues_[column];
            if (childRawNulls_[column] &&
                bits::isBitNull(childRawNulls_[column], rowIndex)) {
              bits::setNull(valueRawNulls, fill);
            } else {
              auto valueIndex = rowIndex;
              if (mayHaveDict && childIndices_[column]) {
                valueIndex = childIndices_[column][rowIndex];
              }
              rawValues[fill] = reinterpret_cast<const V*>(
                  childRawValues_[column])[valueIndex];
            }
            ++fill;
          });
      rawSizes[rowIndex] = fill - rawOffsets[rowIndex];
      startBit += pitch;
    }
    VELOX_CHECK_EQ(fill, totalChildValues);
    std::vector<BufferPtr> allStrings;
    if constexpr (std::is_same_v<V, StringView>) {
      for (auto i = 0; i < childValues_.size(); ++i) {
        std::vector<BufferPtr> strings = vectorStrings(*childValues_[i]);
        allStrings.insert(allStrings.end(), strings.begin(), strings.end());
      }
    }

    valueVector_ = std::make_shared<FlatVector<V>>(
        &memoryPool_,
        requestedType_->type()->childAt(1),
        std::move(valueNulls),
        totalChildValues,
        std::move(values),
        std::move(allStrings));
  }

  static std::vector<BufferPtr> vectorStrings(BaseVector& vector) {
    if (vector.encoding() == VectorEncoding::Simple::CONSTANT) {
      auto buffer =
          vector.asUnchecked<ConstantVector<StringView>>()->getStringBuffer();
      if (!buffer) {
        return {};
      }
      return {buffer};
    } else if (vector.encoding() == VectorEncoding::Simple::FLAT) {
      return vector.asUnchecked<FlatVector<StringView>>()->stringBuffers();
    } else if (vector.encoding() == VectorEncoding::Simple::DICTIONARY) {
      return vector.valueVector()
          ->asUnchecked<FlatVector<StringView>>()
          ->stringBuffers();
    } else {
      VELOX_FAIL("String value is is neither flat, dictionary  nor constant");
    }
  }

  bool fastPath(
      BufferPtr& offsets,
      BufferPtr& sizes,
      RowSet rows,
      const uint64_t* nulls,
      VectorPtr* result) {
    auto& valueType = requestedType_->type()->childAt(1);
    auto valueKind = valueType->kind();
    if (valueKind == TypeKind::MAP || valueKind == TypeKind::ROW ||
        valueKind == TypeKind::ARRAY) {
      return false;
    }
    initKeyValues();
    assert(!children_.empty());
    childRawNulls_.resize(children_.size());
    childRawValues_.resize(children_.size());
    inMap_.resize(children_.size());
    if (!childIndices_.empty()) {
      std::fill(childIndices_.begin(), childIndices_.end(), nullptr);
    }
    SelectivityVector selectedInMap(rows.back() + 1, false);
    for (auto row : rows) {
      selectedInMap.setValid(row, true);
    }
    int32_t totalChildValues = 0;
    rowColumnBits_.resize(bits::nwords(rows.size() * children_.size()));
    std::fill(rowColumnBits_.begin(), rowColumnBits_.end(), 0);
    nullsInChildValues_ = false;
    for (auto i = 0; i < children_.size(); ++i) {
      auto& data = static_cast<const DwrfData&>(children_[i]->formatData());
      inMap_[i] = data.inMap();
      auto& child = childValues_[i];
      childRawNulls_[i] = child->rawNulls();
      if (childRawNulls_[i]) {
        nullsInChildValues_ = true;
      }
      if (child->encoding() == VectorEncoding::Simple::DICTIONARY) {
        childIndices_.resize(children_.size());
        childIndices_[i] = child->wrapInfo()->template as<int32_t>();
        childRawValues_[i] = child->valueVector()->valuesAsVoid();
      } else if (child->encoding() == VectorEncoding::Simple::FLAT) {
        childRawValues_[i] = childValues_[i]->valuesAsVoid();
      } else if (child->encoding() == VectorEncoding::Simple::CONSTANT) {
        if (zeros_.size() < rows.size()) {
          zeros_.resize(rows.size());
        }
        childRawValues_[i] = child->valuesAsVoid();
        if (childValues_[i]->isNullAt(0)) {
          // There are at least rows worth of zero words, so this can serve as
          // an all null bitmap.
          childRawNulls_[i] = reinterpret_cast<const uint64_t*>(zeros_.data());
        } else {
          childRawNulls_[i] = nullptr;
        }
        childIndices_.resize(children_.size());
        // Every null is redirected to row 0, which is valuesAsVoid of constant
        // vector.
        childIndices_[i] = zeros_.data();
      } else {
        VELOX_FAIL(
            "Flat map columns must be flat or single level dictionaries");
      }
      totalChildValues += countInMap(
          inMap_[i], selectedInMap.asRange().bits(), selectedInMap.size(), i);
    }
    BufferPtr keyBuffer =
        AlignedBuffer::allocate<T>(totalChildValues, &memoryPool_);
    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
        fillValues,
        valueKind,
        offsets,
        sizes,
        nulls,
        rows,
        totalChildValues,
        keyBuffer->template asMutable<T>());

    std::vector<BufferPtr> keyStrings;
    if (std::is_same_v<T, StringView> && keyStrings_) {
      keyStrings.push_back(keyStrings_);
    }
    auto keyVector = std::make_shared<FlatVector<T>>(
        &memoryPool_,
        requestedType_->type()->childAt(0),
        BufferPtr(nullptr),
        totalChildValues,
        std::move(keyBuffer),
        std::move(keyStrings));
    *result = std::make_shared<MapVector>(
        &memoryPool_,
        requestedType_->type(),
        resultNulls(),
        rows.size(),
        std::move(offsets),
        std::move(sizes),
        std::move(keyVector),
        std::move(valueVector_));
    return true;
  }

  void initKeyValues() {
    if (!keyValues_.empty()) {
      return;
    }
    assert(!children_.empty());
    keyValues_.resize(children_.size());
    if constexpr (std::is_same_v<T, StringView>) {
      int32_t strKeySize = 0;
      for (int k = 0; k < children_.size(); ++k) {
        if (!keyNodes_[k].key.get().isInline()) {
          strKeySize += keyNodes_[k].key.get().size();
        }
      }
      char* rawStrKeyBuffer = nullptr;
      if (strKeySize > 0) {
        keyStrings_ = AlignedBuffer::allocate<char>(strKeySize, &memoryPool_);
        rawStrKeyBuffer = keyStrings_->template asMutable<char>();
        strKeySize = 0;
      }
      for (int k = 0; k < children_.size(); ++k) {
        auto& s = keyNodes_[k].key.get();
        if (!s.isInline()) {
          memcpy(&rawStrKeyBuffer[strKeySize], s.data(), s.size());
          *reinterpret_cast<StringView*>(&keyValues_[k]) =
              StringView(&rawStrKeyBuffer[strKeySize], s.size());
          strKeySize += s.size();
        } else {
          keyValues_[k] = s;
        }
      }
    } else {
      for (auto i = 0; i < children_.size(); ++i) {
        keyValues_[i] = keyNodes_[i].key.get();
      }
    }
  }

  common::ScanSpec structScanSpec_;
  std::vector<KeyNode<T>> keyNodes_;
  std::vector<VectorPtr> childValues_;
  std::vector<std::vector<BaseVector::CopyRange>> copyRanges_;
  std::vector<const uint64_t*> inMap_;
  std::vector<const uint64_t*> childRawNulls_;
  // if a child is dictionary encoded, these are indices of non-null values.
  std::vector<const int32_t*> childIndices_;
  std::vector<const void*> childRawValues_;
  std::vector<uint64_t> rowColumnBits_;
  std::vector<T> keyValues_;
  BufferPtr keyStrings_;
  bool nullsInChildValues_{false};
  VectorPtr valueVector_;
  std::vector<int32_t> zeros_;
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
