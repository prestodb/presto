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

#include "velox/common/base/Exceptions.h"
#include "velox/expression/EvalCtx.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::velox::functions {

struct MakeRowFromMapOptions {
  /// The list of keys to extract from each map. The type should be the same
  /// as the key type in the map and the typeKind template specified when
  /// creating MakeRowFromMap object. Should be the same size as
  /// 'outputFieldNames'. Cannot be empty or null.
  VectorPtr keysToProject;

  /// The names to assign to the corresponding fields in the output RowVector.
  ///  The i-th key in 'keysToProject' will be mapped to the i-th field name and
  ///  therefore it should be the same size as 'keysToProject'. Cannot contain
  ///  empty or duplicate entries.
  std::vector<std::string> outputFieldNames;

  /// When set to false, if the map is null, a key is missing, or the
  /// value is null, the output field will also be null.
  /// If set to true, replaces null output values with type-specific defaults
  /// like for primitive types, they will be set to the c++ default
  /// initialized value, like 0 for INTEGER and empty string for VARCHAR. For
  /// Array/Map they will be set to an empty array or map. Not supported for
  /// ROW or UNKNOWN types.
  bool replaceNulls{false};

  /// Only valid if replaceNulls is false. If set to true, a null map will
  /// result in a null row in the output. Otherwise, the top level row will
  /// not be null but every field in the output row will be null.
  bool allowTopLevelNulls{false};

  /// If true, duplicate keys are allowed and the last value is used.
  bool throwOnDuplicateKeys{true};
};

///  A utility class for projecting specific keys from a vector of MAP type
///  (which may be encoded) into a RowVector with named fields corresponding to
///  each projected key. For each active row, it extracts the values
///  corresponding to the specified keys and assigns them to their respective
///  fields in an output RowVector. The output RowVector contains one field for
///  each specified key, and its size will be equal to the last active row in
///  the selectivity vector. It optionally accepts an exec::EvalCtx object
///  pointer for use within Vector Functions. This allows it to leverage the
///  vector and decodedVector pool within that context and apply per-row error
///  handling as required by expression evaluation.
///
///  Note:
/// - Only keys of types SMALLINT, INTEGER, and BIGINT are currently
/// supported. TODO: Support other key types
/// (https://github.com/facebookincubator/velox/issues/14790)
/// - Rows marked as inactive in the 'rows' selectivity vector will result in
/// valid rows in the output; however, no guarantees are made regarding their
/// assigned values.
template <TypeKind KeyKind>
class MakeRowFromMap {
  using KeyType = typename TypeTraits<KeyKind>::NativeType;

 public:
  MakeRowFromMap(const MakeRowFromMapOptions& options)
      : replaceNulls_(options.replaceNulls),
        allowTopLevelNulls_(options.allowTopLevelNulls),
        throwOnDuplicateKeys_(options.throwOnDuplicateKeys),
        outputFieldNames_(options.outputFieldNames),
        inputKeyType_(options.keysToProject->type()) {
    VELOX_USER_CHECK_NOT_NULL(options.keysToProject);
    VELOX_USER_CHECK_GT(
        options.keysToProject->size(), 0, "Keys to project cannot be empty");
    VELOX_USER_CHECK_EQ(
        options.keysToProject->size(),
        options.outputFieldNames.size(),
        "Number of keys to project and output field names must be the same");
    ensureValidFieldNames(options.outputFieldNames);

    VELOX_USER_CHECK_EQ(
        KeyKind, inputKeyType_->kind(), "Unexptected key TypeKind");
    if (inputKeyType_ != SMALLINT() && inputKeyType_ != INTEGER() &&
        inputKeyType_ != BIGINT()) {
      VELOX_NYI(
          "Only SMALLINT, INTEGER, BIGINT keys are currently supported, instead got {}",
          inputKeyType_->toString());
    }
    createKeyToFieldIndexMap(options.keysToProject);
  }

  VectorPtr apply(
      const BaseVector& map,
      const SelectivityVector& rows,
      exec::EvalCtx* evalCtx) {
    VELOX_USER_CHECK_EQ(
        map.type()->kind(), TypeKind::MAP, "Input must be of MAP typeKind");

    // Ensure that the key type from the map and type of keysToProject is the
    // same.
    const auto& keyType = map.type()->asMap().keyType();
    VELOX_USER_CHECK_EQ(
        keyType,
        inputKeyType_,
        "Map key type and the type of keys to project are not the same");

    return toRowVectorImpl(map, rows, evalCtx);
  }

 private:
  void createKeyToFieldIndexMap(const VectorPtr& keysToProject) {
    keyToIndex_.reserve(keysToProject->size());
    auto keysToProjectVec = keysToProject->template as<SimpleVector<KeyType>>();
    for (size_t i = 0; i < keysToProjectVec->size(); ++i) {
      VELOX_USER_CHECK(
          !keysToProjectVec->isNullAt(i),
          "Keys to project cannot contain null");
      auto key = keysToProjectVec->valueAt(i);
      bool ok = keyToIndex_.insert({key, i}).second;
      // Key must be unique.
      VELOX_USER_CHECK(ok, "Duplicate keys cannot be projected: {}", key);
    }
  }

  template <TypeKind ValueKind>
  static void preFillDefaults(VectorPtr& child) {
    VELOX_CHECK_NOT_NULL(child);
    if constexpr (
        (TypeTraits<ValueKind>::isPrimitiveType ||
         ValueKind == TypeKind::OPAQUE) &&
        ValueKind != TypeKind::UNKNOWN) {
      using NativeType = typename TypeTraits<ValueKind>::NativeType;
      VELOX_CHECK(child->isFlatEncoding());
      auto* rawValues = child->asFlatVector<NativeType>()->mutableRawValues();
      std::fill(rawValues, rawValues + child->size(), NativeType());
    } else if constexpr (
        ValueKind == TypeKind::ARRAY || ValueKind == TypeKind::MAP) {
      auto childSize = child->size();
      auto arrayBaseVector = child->asChecked<ArrayVectorBase>();
      VELOX_CHECK_NOT_NULL(arrayBaseVector);
      auto* rawOffsets = arrayBaseVector->mutableOffsets(childSize)
                             ->asMutable<vector_size_t>();
      auto* rawSizes =
          arrayBaseVector->mutableSizes(childSize)->asMutable<vector_size_t>();
      std::fill(rawOffsets, rawOffsets + childSize, 0);
      std::fill(rawSizes, rawSizes + childSize, 0);
    } else {
      VELOX_USER_FAIL(
          "Unsupported type for replacing nulls: {}",
          child->type()->toString());
    }
  }

  VectorPtr toRowVectorImpl(
      const BaseVector& vector,
      const SelectivityVector& rows,
      exec::EvalCtx* evalCtx) {
    exec::LocalDecodedVector decodedMap(evalCtx);
    decodedMap.get()->decode(vector, rows);
    auto mapBase = decodedMap->base()->asUnchecked<MapVector>();
    exec::LocalDecodedVector decodedKeys(evalCtx);
    decodedKeys.get()->decode(*mapBase->mapKeys());
    exec::LocalDecodedVector decodedValues(evalCtx);
    decodedValues.get()->decode(*mapBase->mapValues());
    auto valueType = mapBase->mapValues()->type();
    auto* offsets = mapBase->rawOffsets();
    auto* sizes = mapBase->rawSizes();
    auto outputSize = rows.end();

    std::vector<VectorPtr> children;
    children.reserve(keyToIndex_.size());
    for (size_t i = 0; i < keyToIndex_.size(); ++i) {
      if (evalCtx) {
        children.push_back(evalCtx->vectorPool()->get(valueType, outputSize));
      } else {
        children.push_back(
            BaseVector::create(valueType, outputSize, mapBase->pool()));
      }
      if (!replaceNulls_) {
        auto rawNulls = children.back()->mutableRawNulls();
        bits::fillBits(rawNulls, 0, outputSize, bits::kNull);
      } else {
        VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
            preFillDefaults, valueType->kind(), children.back());
      }
    }
    auto outputNulls =
        (!replaceNulls_ && allowTopLevelNulls_ && decodedMap->mayHaveNulls())
        ? allocateNulls(outputSize, mapBase->pool(), bits::kNotNull)
        : nullptr;
    auto* rawNulls =
        outputNulls ? outputNulls->template asMutable<uint64_t>() : nullptr;
    std::vector<bool> visited(keyToIndex_.size());
    std::vector<std::vector<BaseVector::CopyRange>> copyRangesFromBase(
        keyToIndex_.size());
    rows.applyToSelected([&](vector_size_t row) INLINE_LAMBDA {
      std::fill(visited.begin(), visited.end(), false);
      if (!decodedMap->isNullAt(row)) {
        auto decodedIndex = decodedMap->index(row);
        auto offset = offsets[decodedIndex];
        auto size = sizes[decodedIndex];
        for (auto i = offset; i < offset + size; ++i) {
          if (decodedKeys->isNullAt(i) || decodedValues->isNullAt(i)) {
            continue;
          }
          auto key = decodedKeys->valueAt<KeyType>(i);
          auto it = keyToIndex_.find(key);
          if (it == keyToIndex_.end()) {
            continue;
          }
          auto index = it->second;
          if (visited[index]) {
            if (throwOnDuplicateKeys_) {
              auto errorMessage =
                  fmt::format("Duplicate keys not allowed: {}", key);
              if (evalCtx) {
                evalCtx->setStatus(row, Status::UserError(errorMessage));
              } else {
                VELOX_USER_FAIL(errorMessage);
              }
              return;
            }
            VELOX_CHECK(!copyRangesFromBase[index].empty());
            VELOX_CHECK_EQ(copyRangesFromBase[index].back().targetIndex, row);
            copyRangesFromBase[index].back().sourceIndex =
                decodedValues->index(i);
          } else {
            visited[index] = true;
            copyRangesFromBase[index].push_back(
                {decodedValues->index(i), row, 1});
          }
        }
      } else if (rawNulls) {
        bits::setNull(rawNulls, row, true);
      }
    });
    for (size_t i = 0; i < children.size(); ++i) {
      children[i]->copyRanges(decodedValues->base(), copyRangesFromBase[i]);
    }

    return std::make_shared<RowVector>(
        vector.pool(),
        ROW(outputFieldNames_, valueType),
        std::move(outputNulls),
        outputSize,
        std::move(children));
  }

  void ensureValidFieldNames(const std::vector<std::string>& fieldNames) {
    std::unordered_set<std::string> fieldNamesSet;
    for (const auto& fieldName : fieldNames) {
      VELOX_USER_CHECK(!fieldName.empty(), "Field name cannot be empty");
      auto ok = fieldNamesSet.insert(fieldName).second;
      VELOX_USER_CHECK(
          ok, "Duplicate field names are not allowed: {}", fieldName);
    }
  }

  const bool replaceNulls_{false};
  const bool allowTopLevelNulls_{false};
  const bool throwOnDuplicateKeys_{true};
  const std::vector<std::string> outputFieldNames_;
  const TypePtr inputKeyType_;
  std::unordered_map<KeyType, size_t> keyToIndex_;
};
} // namespace facebook::velox::functions
