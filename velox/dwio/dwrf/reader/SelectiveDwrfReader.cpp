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

#include "velox/dwio/dwrf/reader/SelectiveDwrfReader.h"
#include "velox/dwio/common/TypeUtils.h"

#include "velox/dwio/dwrf/reader/SelectiveByteRleColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveDecimalColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveFlatMapColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveFloatingPointColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveIntegerDictionaryColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveIntegerDirectColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveRepeatedColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveStringDictionaryColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveStringDirectColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveStructColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveTimestampColumnReader.h"

namespace facebook::velox::dwrf {

using namespace facebook::velox::dwio::common;

std::unique_ptr<SelectiveColumnReader> buildIntegerReader(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    DwrfParams& params,
    uint32_t numBytes,
    common::ScanSpec& scanSpec) {
  const EncodingKey encodingKey{
      fileType->id(), params.flatMapContext().sequence};
  auto& stripe = params.stripeStreams();
  const auto encodingKind =
      static_cast<int64_t>(stripe.getEncoding(encodingKey).kind());
  switch (encodingKind) {
    case proto::ColumnEncoding_Kind_DICTIONARY:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
      return std::make_unique<SelectiveIntegerDictionaryColumnReader>(
          requestedType, fileType, params, scanSpec, numBytes);
    case proto::ColumnEncoding_Kind_DIRECT:
    case proto::ColumnEncoding_Kind_DIRECT_V2:
      return std::make_unique<SelectiveIntegerDirectColumnReader>(
          requestedType, fileType, params, numBytes, scanSpec);
    default:
      VELOX_FAIL("buildReader unhandled integer encoding: {}", encodingKind);
  }
}

// static
std::unique_ptr<SelectiveColumnReader> SelectiveDwrfReader::build(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    DwrfParams& params,
    common::ScanSpec& scanSpec,
    bool isRoot) {
  VELOX_CHECK(
      !isRoot || fileType->type()->kind() == TypeKind::ROW,
      "The root object can only be a row.");

  dwio::common::typeutils::checkTypeCompatibility(
      *fileType->type(), *requestedType);
  EncodingKey ek{fileType->id(), params.flatMapContext().sequence};
  auto& stripe = params.stripeStreams();
  switch (fileType->type()->kind()) {
    case TypeKind::INTEGER:
      return buildIntegerReader(
          requestedType, fileType, params, INT_BYTE_SIZE, scanSpec);
    case TypeKind::BIGINT:
      if (fileType->type()->isDecimal()) {
        return std::make_unique<SelectiveDecimalColumnReader<int64_t>>(
            fileType, params, scanSpec);
      } else {
        return buildIntegerReader(
            requestedType, fileType, params, LONG_BYTE_SIZE, scanSpec);
      }
    case TypeKind::SMALLINT:
      return buildIntegerReader(
          requestedType, fileType, params, SHORT_BYTE_SIZE, scanSpec);
    case TypeKind::ARRAY:
      return std::make_unique<SelectiveListColumnReader>(
          requestedType, fileType, params, scanSpec);
    case TypeKind::MAP:
      if (stripe.getEncoding(ek).kind() ==
          proto::ColumnEncoding_Kind_MAP_FLAT) {
        return createSelectiveFlatMapColumnReader(
            requestedType, fileType, params, scanSpec);
      }
      return std::make_unique<SelectiveMapColumnReader>(
          requestedType, fileType, params, scanSpec);
    case TypeKind::REAL:
      if (requestedType->kind() == TypeKind::REAL) {
        return std::make_unique<
            SelectiveFloatingPointColumnReader<float, float>>(
            requestedType, fileType, params, scanSpec);
      } else {
        return std::make_unique<
            SelectiveFloatingPointColumnReader<float, double>>(
            requestedType, fileType, params, scanSpec);
      }
    case TypeKind::DOUBLE:
      return std::make_unique<
          SelectiveFloatingPointColumnReader<double, double>>(
          requestedType, fileType, params, scanSpec);
    case TypeKind::ROW:
      return std::make_unique<SelectiveStructColumnReader>(
          requestedType, fileType, params, scanSpec, isRoot);
    case TypeKind::BOOLEAN:
      return std::make_unique<SelectiveByteRleColumnReader>(
          requestedType, fileType, params, scanSpec, true);
    case TypeKind::TINYINT:
      return std::make_unique<SelectiveByteRleColumnReader>(
          requestedType, fileType, params, scanSpec, false);
    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR:
      switch (static_cast<int64_t>(stripe.getEncoding(ek).kind())) {
        case proto::ColumnEncoding_Kind_DIRECT:
        case proto::ColumnEncoding_Kind_DIRECT_V2:
          return std::make_unique<SelectiveStringDirectColumnReader>(
              fileType, params, scanSpec);
        case proto::ColumnEncoding_Kind_DICTIONARY:
        case proto::ColumnEncoding_Kind_DICTIONARY_V2:
          return std::make_unique<SelectiveStringDictionaryColumnReader>(
              fileType, params, scanSpec);
        default:
          DWIO_RAISE("buildReader string unknown encoding");
      }
    case TypeKind::TIMESTAMP:
      return std::make_unique<SelectiveTimestampColumnReader>(
          fileType, params, scanSpec);
    case TypeKind::HUGEINT:
      if (fileType->type()->isDecimal()) {
        return std::make_unique<SelectiveDecimalColumnReader<int128_t>>(
            fileType, params, scanSpec);
      }
      [[fallthrough]];
    default:
      VELOX_FAIL(
          "buildReader unhandled type: " +
          mapTypeKindToName(fileType->type()->kind()));
  }
}

} // namespace facebook::velox::dwrf
