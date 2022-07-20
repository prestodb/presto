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
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
    DwrfParams& params,
    uint32_t numBytes,
    common::ScanSpec& scanSpec) {
  EncodingKey ek{requestedType->id, params.flatMapContext().sequence};
  auto& stripe = params.stripeStreams();
  switch (static_cast<int64_t>(stripe.getEncoding(ek).kind())) {
    case proto::ColumnEncoding_Kind_DICTIONARY:
      return std::make_unique<SelectiveIntegerDictionaryColumnReader>(
          requestedType, dataType, params, scanSpec, numBytes);
    case proto::ColumnEncoding_Kind_DIRECT:
      return std::make_unique<SelectiveIntegerDirectColumnReader>(
          requestedType, dataType, params, numBytes, scanSpec);
    default:
      DWIO_RAISE("buildReader unhandled integer encoding");
  }
}

// static
std::unique_ptr<SelectiveColumnReader> SelectiveDwrfReader::build(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
    DwrfParams& params,
    common::ScanSpec& scanSpec) {
  dwio::common::typeutils::CompatChecker::check(
      *dataType->type, *requestedType->type);
  EncodingKey ek{dataType->id, params.flatMapContext().sequence};
  auto& stripe = params.stripeStreams();
  switch (dataType->type->kind()) {
    case TypeKind::INTEGER:
      return buildIntegerReader(
          requestedType, dataType, params, INT_BYTE_SIZE, scanSpec);
    case TypeKind::BIGINT:
      return buildIntegerReader(
          requestedType, dataType, params, LONG_BYTE_SIZE, scanSpec);
    case TypeKind::SMALLINT:
      return buildIntegerReader(
          requestedType, dataType, params, SHORT_BYTE_SIZE, scanSpec);
    case TypeKind::ARRAY:
      return std::make_unique<SelectiveListColumnReader>(
          requestedType, dataType, params, scanSpec);
    case TypeKind::MAP:
      if (stripe.getEncoding(ek).kind() ==
          proto::ColumnEncoding_Kind_MAP_FLAT) {
        VELOX_UNSUPPORTED("SelectiveColumnReader does not support flat maps");
      }
      return std::make_unique<SelectiveMapColumnReader>(
          requestedType, dataType, params, scanSpec);
    case TypeKind::REAL:
      if (requestedType->type->kind() == TypeKind::REAL) {
        return std::make_unique<
            SelectiveFloatingPointColumnReader<float, float>>(
            requestedType, params, scanSpec);
      } else {
        return std::make_unique<
            SelectiveFloatingPointColumnReader<float, double>>(
            requestedType, params, scanSpec);
      }
    case TypeKind::DOUBLE:
      return std::make_unique<
          SelectiveFloatingPointColumnReader<double, double>>(
          requestedType, params, scanSpec);
    case TypeKind::ROW:
      return std::make_unique<SelectiveStructColumnReader>(
          requestedType, dataType, params, scanSpec);
    case TypeKind::BOOLEAN:
      return std::make_unique<SelectiveByteRleColumnReader>(
          requestedType, dataType, params, scanSpec, true);
    case TypeKind::TINYINT:
      return std::make_unique<SelectiveByteRleColumnReader>(
          requestedType, dataType, params, scanSpec, false);
    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR:
      switch (static_cast<int64_t>(stripe.getEncoding(ek).kind())) {
        case proto::ColumnEncoding_Kind_DIRECT:
          return std::make_unique<SelectiveStringDirectColumnReader>(
              requestedType, params, scanSpec);
        case proto::ColumnEncoding_Kind_DICTIONARY:
          return std::make_unique<SelectiveStringDictionaryColumnReader>(
              requestedType, params, scanSpec);
        default:
          DWIO_RAISE("buildReader string unknown encoding");
      }
    case TypeKind::TIMESTAMP:
      return std::make_unique<SelectiveTimestampColumnReader>(
          requestedType, params, scanSpec);
    default:
      DWIO_RAISE(
          "buildReader unhandled type: " +
          mapTypeKindToName(dataType->type->kind()));
  }
}

} // namespace facebook::velox::dwrf
