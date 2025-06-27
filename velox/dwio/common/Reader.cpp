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

#include "velox/dwio/common/Reader.h"

namespace facebook::velox::dwio::common {

using namespace velox::common;

VectorPtr RowReader::projectColumns(
    const VectorPtr& input,
    const ScanSpec& spec,
    const Mutation* mutation) {
  auto* inputRow = input->as<RowVector>();
  VELOX_CHECK_NOT_NULL(inputRow);
  auto& inputRowType = input->type()->asRow();
  column_index_t numColumns = 0;
  for (auto& childSpec : spec.children()) {
    numColumns = std::max(numColumns, childSpec->channel() + 1);
  }
  std::vector<std::string> names(numColumns);
  std::vector<TypePtr> types(numColumns);
  std::vector<VectorPtr> children(numColumns);
  std::vector<uint64_t> passed(bits::nwords(input->size()), -1);
  if (mutation) {
    if (mutation->deletedRows) {
      bits::andWithNegatedBits(
          passed.data(), mutation->deletedRows, 0, input->size());
    }
    if (mutation->randomSkip) {
      bits::forEachSetBit(passed.data(), 0, input->size(), [&](auto i) {
        if (!mutation->randomSkip->testOne()) {
          bits::clearBit(passed.data(), i);
        }
      });
    }
  }
  for (auto& childSpec : spec.children()) {
    VELOX_CHECK_NULL(childSpec->deltaUpdate());
    VectorPtr child;
    TypePtr childType;
    if (childSpec->isConstant()) {
      child = BaseVector::wrapInConstant(
          input->size(), 0, childSpec->constantValue());
      childType = child->type();
    } else {
      auto childIdx = inputRowType.getChildIdx(childSpec->fieldName());
      childType = inputRowType.childAt(childIdx);
      child = inputRow->childAt(childIdx);
      if (child) {
        childSpec->applyFilter(*child, inputRow->size(), passed.data());
      }
    }
    if (!childSpec->projectOut()) {
      continue;
    }
    auto i = childSpec->channel();
    names[i] = childSpec->fieldName();
    types[i] = childType;
    children[i] = std::move(child);
  }
  auto rowType = ROW(std::move(names), std::move(types));
  auto size = bits::countBits(passed.data(), 0, input->size());
  if (size == 0) {
    return RowVector::createEmpty(rowType, input->pool());
  }
  if (size < input->size()) {
    auto indices = allocateIndices(size, input->pool());
    auto* rawIndices = indices->asMutable<vector_size_t>();
    vector_size_t j = 0;
    bits::forEachSetBit(
        passed.data(), 0, input->size(), [&](auto i) { rawIndices[j++] = i; });
    for (auto& child : children) {
      if (!child) {
        continue;
      }
      child->disableMemo();
      child = BaseVector::wrapInDictionary(
          nullptr, indices, size, std::move(child));
    }
  }
  return std::make_shared<RowVector>(
      input->pool(), rowType, nullptr, size, std::move(children));
}

namespace {
void fillRowNumberVector(
    VectorPtr& rowNumVector,
    uint64_t previousRow,
    uint64_t rowsToRead,
    const dwio::common::SelectiveColumnReader* columnReader,
    VectorPtr& result) {
  FlatVector<int64_t>* flatRowNum{nullptr};
  if (rowNumVector && BaseVector::isVectorWritable(rowNumVector)) {
    flatRowNum = rowNumVector->asFlatVector<int64_t>();
  }
  if (flatRowNum) {
    flatRowNum->clearAllNulls();
    flatRowNum->resize(result->size());
  } else {
    rowNumVector = std::make_shared<FlatVector<int64_t>>(
        result->pool(),
        BIGINT(),
        nullptr,
        result->size(),
        AlignedBuffer::allocate<int64_t>(result->size(), result->pool()),
        std::vector<BufferPtr>());
    flatRowNum = rowNumVector->asUnchecked<FlatVector<int64_t>>();
  }
  auto* rawRowNum = flatRowNum->mutableRawValues();
  const auto rowOffsets = columnReader->outputRows();
  VELOX_DCHECK_EQ(rowOffsets.size(), result->size());
  for (int i = 0; i < rowOffsets.size(); ++i) {
    rawRowNum[i] = previousRow + rowOffsets[i];
  }
}
} // namespace

void RowReader::readWithRowNumber(
    std::unique_ptr<dwio::common::SelectiveColumnReader>& columnReader,
    const dwio::common::RowReaderOptions& options,
    uint64_t previousRow,
    uint64_t rowsToRead,
    const dwio::common::Mutation* mutation,
    VectorPtr& result) {
  const auto& rowNumberColumnInfo = options.rowNumberColumnInfo();
  VELOX_CHECK(rowNumberColumnInfo.has_value());
  const auto rowNumberColumnIndex = rowNumberColumnInfo->insertPosition;
  const auto& rowNumberColumnName = rowNumberColumnInfo->name;
  column_index_t numChildren{0};
  for (auto& column : options.scanSpec()->children()) {
    if (column->keepValues()) {
      ++numChildren;
    }
  }
  VELOX_CHECK_LE(rowNumberColumnIndex, numChildren);
  auto* rowVector = result->asUnchecked<RowVector>();
  VectorPtr rowNumVector;
  if (rowVector->childrenSize() != numChildren) {
    VELOX_CHECK_EQ(rowVector->childrenSize(), numChildren + 1);
    rowNumVector = rowVector->childAt(rowNumberColumnIndex);
    const auto& rowType = rowVector->type()->asRow();
    auto names = rowType.names();
    auto types = rowType.children();
    auto children = rowVector->children();
    VELOX_DCHECK(!names.empty() && !types.empty() && !children.empty());
    names.erase(names.begin() + rowNumberColumnIndex);
    types.erase(types.begin() + rowNumberColumnIndex);
    children.erase(children.begin() + rowNumberColumnIndex);
    result = std::make_shared<RowVector>(
        rowVector->pool(),
        ROW(std::move(names), std::move(types)),
        rowVector->nulls(),
        rowVector->size(),
        std::move(children));
  }
  columnReader->next(rowsToRead, result, mutation);
  fillRowNumberVector(
      rowNumVector, previousRow, rowsToRead, columnReader.get(), result);
  rowVector = result->asUnchecked<RowVector>();
  auto& rowType = rowVector->type()->asRow();
  auto names = rowType.names();
  auto types = rowType.children();
  auto children = rowVector->children();
  names.insert(names.begin() + rowNumberColumnIndex, rowNumberColumnName);
  types.insert(types.begin() + rowNumberColumnIndex, BIGINT());
  children.insert(children.begin() + rowNumberColumnIndex, rowNumVector);
  result = std::make_shared<RowVector>(
      rowVector->pool(),
      ROW(std::move(names), std::move(types)),
      rowVector->nulls(),
      rowVector->size(),
      std::move(children));
}

namespace {

void logTypeInequality(
    const Type& fileType,
    const Type& tableType,
    const std::string& fileFieldName,
    const std::string& tableFieldName) {
  VLOG(1) << "Type of the File field '" << fileFieldName
          << "' does not match the type of the Table field '" << tableFieldName
          << "': [" << fileType.toString() << "] vs [" << tableType.toString()
          << "]";
}

// Forward declaration for general type tree recursion function.
TypePtr updateColumnNamesImpl(
    const TypePtr& fileType,
    const TypePtr& tableType,
    const std::string& fileFieldName,
    const std::string& tableFieldName);

// Non-primitive type tree recursion function.
template <typename T>
TypePtr updateColumnNamesImpl(
    const TypePtr& fileType,
    const TypePtr& tableType) {
  const auto fileRowType = std::dynamic_pointer_cast<const T>(fileType);
  const auto tableRowType = std::dynamic_pointer_cast<const T>(tableType);

  std::vector<std::string> newFileFieldNames;
  newFileFieldNames.reserve(fileRowType->size());
  std::vector<TypePtr> newFileFieldTypes;
  newFileFieldTypes.reserve(fileRowType->size());

  for (auto childIdx = 0; childIdx < tableRowType->size(); ++childIdx) {
    if (childIdx >= fileRowType->size()) {
      break;
    }

    newFileFieldTypes.push_back(updateColumnNamesImpl(
        fileRowType->childAt(childIdx),
        tableRowType->childAt(childIdx),
        fileRowType->nameOf(childIdx),
        tableRowType->nameOf(childIdx)));

    newFileFieldNames.push_back(tableRowType->nameOf(childIdx));
  }

  for (auto childIdx = tableRowType->size(); childIdx < fileRowType->size();
       ++childIdx) {
    newFileFieldTypes.push_back(fileRowType->childAt(childIdx));
    newFileFieldNames.push_back(fileRowType->nameOf(childIdx));
  }

  return std::make_shared<const T>(
      std::move(newFileFieldNames), std::move(newFileFieldTypes));
}

// General type tree recursion function.
TypePtr updateColumnNamesImpl(
    const TypePtr& fileType,
    const TypePtr& tableType,
    const std::string& fileFieldName,
    const std::string& tableFieldName) {
  // Check type kind equality. If not equal, no point to continue down the
  // tree.
  if (fileType->kind() != tableType->kind()) {
    logTypeInequality(*fileType, *tableType, fileFieldName, tableFieldName);
    return fileType;
  }

  // For leaf types we return type as is.
  if (fileType->isPrimitiveType()) {
    return fileType;
  }

  if (fileType->isRow()) {
    return updateColumnNamesImpl<RowType>(fileType, tableType);
  }

  if (fileType->isMap()) {
    return updateColumnNamesImpl<MapType>(fileType, tableType);
  }

  if (fileType->isArray()) {
    return updateColumnNamesImpl<ArrayType>(fileType, tableType);
  }

  // We should not be here.
  VLOG(1) << "Unexpected table type during column names update for File field '"
          << fileFieldName << "': [" << fileType->toString() << "]";
  return fileType;
}
} // namespace

TypePtr Reader::updateColumnNames(
    const TypePtr& fileType,
    const TypePtr& tableType) {
  return updateColumnNamesImpl(fileType, tableType, "", "");
}

} // namespace facebook::velox::dwio::common
