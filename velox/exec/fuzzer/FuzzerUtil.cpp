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
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include <filesystem>
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/Writer.h"

namespace facebook::velox::exec::test {
void writeToFile(
    const std::string& path,
    const VectorPtr& vector,
    memory::MemoryPool* pool) {
  dwrf::WriterOptions options;
  options.schema = vector->type();
  options.memoryPool = pool;
  auto writeFile = std::make_unique<LocalWriteFile>(path, true, false);
  auto sink =
      std::make_unique<dwio::common::WriteFileSink>(std::move(writeFile), path);
  dwrf::Writer writer(std::move(sink), options);
  writer.write(vector);
  writer.close();
}

std::vector<Split> makeSplits(
    const std::vector<RowVectorPtr>& inputs,
    const std::string& path,
    const std::shared_ptr<memory::MemoryPool>& writerPool) {
  std::vector<Split> splits;
  for (auto i = 0; i < inputs.size(); ++i) {
    const std::string filePath = fmt::format("{}/{}", path, i);
    writeToFile(filePath, inputs[i], writerPool.get());
    splits.push_back(makeSplit(filePath));
  }

  return splits;
}

std::vector<Split> makeSplits(const std::string& directory) {
  std::vector<Split> splits;
  for (auto const& p : std::filesystem::directory_iterator{directory}) {
    VELOX_CHECK(!p.is_directory());
    splits.emplace_back(makeSplit(p.path().string()));
  }
  return splits;
}

Split makeSplit(const std::string& filePath) {
  return Split{std::make_shared<connector::hive::HiveConnectorSplit>(
      kHiveConnectorId, filePath, dwio::common::FileFormat::DWRF)};
}

std::shared_ptr<connector::ConnectorSplit> makeConnectorSplit(
    const std::string& filePath) {
  return std::make_shared<connector::hive::HiveConnectorSplit>(
      kHiveConnectorId, filePath, dwio::common::FileFormat::DWRF);
}

std::vector<std::string> makeNames(const std::string& prefix, size_t n) {
  std::vector<std::string> names;
  names.reserve(n);
  for (auto i = 0; i < n; ++i) {
    names.push_back(fmt::format("{}{}", prefix, i));
  }
  return names;
}

RowTypePtr concat(const RowTypePtr& a, const RowTypePtr& b) {
  std::vector<std::string> names = a->names();
  std::vector<TypePtr> types = a->children();

  for (auto i = 0; i < b->size(); ++i) {
    names.push_back(b->nameOf(i));
    types.push_back(b->childAt(i));
  }

  return ROW(std::move(names), std::move(types));
}

// Sometimes we generate zero-column input of type ROW({}) or a column of type
// UNKNOWN(). Such data cannot be written to a file and therefore cannot
// be tested with TableScan.
bool isTableScanSupported(const TypePtr& type) {
  if (type->kind() == TypeKind::ROW && type->size() == 0) {
    return false;
  }
  if (type->kind() == TypeKind::UNKNOWN) {
    return false;
  }
  if (type->kind() == TypeKind::HUGEINT) {
    return false;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (!isTableScanSupported(type->childAt(i))) {
      return false;
    }
  }

  return true;
}

bool containsType(const TypePtr& type, const TypePtr& search) {
  if (type->equivalent(*search)) {
    return true;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (containsType(type->childAt(i), search)) {
      return true;
    }
  }
  return false;
}

bool containsTypeKind(const TypePtr& type, const TypeKind& search) {
  if (type->kind() == search) {
    return true;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (containsTypeKind(type->childAt(i), search)) {
      return true;
    }
  }

  return false;
}

bool containsUnsupportedTypes(const TypePtr& type) {
  return containsTypeKind(type, TypeKind::TIMESTAMP) ||
      containsTypeKind(type, TypeKind::VARBINARY) ||
      containsType(type, INTERVAL_DAY_TIME());
}
} // namespace facebook::velox::exec::test
