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
    const TypePtr& requestedType,
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

  common::ScanSpec* keysSpec = nullptr;
  common::ScanSpec* valuesSpec = nullptr;
  std::unordered_map<KeyValue<T>, common::ScanSpec*, KeyValueHash<T>>
      childSpecs;
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
  } else {
    for (auto& c : scanSpec.children()) {
      T key;
      if constexpr (std::is_same_v<T, StringView>) {
        key = StringView(c->fieldName());
      } else {
        key = folly::to<T>(c->fieldName());
      }
      childSpecs[KeyValue<T>(key)] = c.get();
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

  if (!asStruct) {
    std::sort(keyNodes.begin(), keyNodes.end(), [](auto& x, auto& y) {
      return x.sequence < y.sequence;
    });
  }
  return keyNodes;
}

template <typename T>
class SelectiveFlatMapAsStructReader : public SelectiveStructColumnReaderBase {
 public:
  SelectiveFlatMapAsStructReader(
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec)
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
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec)
      : SelectiveStructColumnReaderBase(
            requestedType,
            fileType,
            params,
            scanSpec),
        flatMap_(
            *this,
            getKeyNodes<T>(requestedType, fileType, params, scanSpec, false)) {}

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override {
    flatMap_.read(offset, rows, incomingNulls);
  }

  void getValues(RowSet rows, VectorPtr* result) override {
    flatMap_.getValues(rows, result);
  }

 private:
  dwio::common::SelectiveFlatMapColumnReaderHelper<T, KeyNode<T>, DwrfData>
      flatMap_;
};

template <typename T>
std::unique_ptr<dwio::common::SelectiveColumnReader> createReader(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    DwrfParams& params,
    common::ScanSpec& scanSpec) {
  if (scanSpec.isFlatMapAsStruct()) {
    return std::make_unique<SelectiveFlatMapAsStructReader<T>>(
        requestedType, fileType, params, scanSpec);
  } else {
    return std::make_unique<SelectiveFlatMapReader<T>>(
        requestedType, fileType, params, scanSpec);
  }
}

} // namespace

std::unique_ptr<dwio::common::SelectiveColumnReader>
createSelectiveFlatMapColumnReader(
    const TypePtr& requestedType,
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
