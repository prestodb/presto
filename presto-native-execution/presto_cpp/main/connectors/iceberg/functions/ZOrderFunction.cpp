/*
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
#include "presto_cpp/main/connectors/iceberg/functions/ZOrderFunction.h"

#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <optional>

#include "velox/expression/DecodedArgs.h"
#include "velox/expression/StringWriter.h"
#include "velox/expression/VectorFunction.h"

using namespace facebook::velox;

namespace facebook::presto::iceberg::functions {
namespace {

// Ported from the Presto Java Iceberg connector's ZOrderByteUtils and
// IcebergZOrderFunctions, which follow Apache Iceberg's ZOrderByteUtils. Every
// difference in bytes changes the order of z-order rewritten data files, so the
// encoding here must stay identical to Java's.

// Bytes in the encoding of a numeric, boolean, date or timestamp field, and of
// any null field.
constexpr int32_t kPrimitiveSize = 8;
// Bytes in the encoding of a varchar or varbinary field.
constexpr int32_t kStringSize = 32;

void writeBigEndian(uint64_t value, char* out) {
  for (int32_t i = kPrimitiveSize - 1; i >= 0; --i) {
    out[i] = static_cast<char>(value & 0xFF);
    value >>= 8;
  }
}

// Flips the sign bit so that negative values order before positive ones.
void encodeWholeNumber(int64_t value, char* out) {
  writeBigEndian(static_cast<uint64_t>(value) ^ 0x8000000000000000ULL, out);
}

// Reinterprets the IEEE 754 bits as a sign-magnitude integer, which orders the
// same way as the floating point value. The shift by 31 and the canonical NaN
// match Java's Double.doubleToLongBits and Iceberg's ZOrderByteUtils.
void encodeFloatingPoint(double value, char* out) {
  auto bits =
      std::isnan(value) ? 0x7ff8000000000000LL : std::bit_cast<int64_t>(value);
  bits ^= (bits >> 31) | std::numeric_limits<int64_t>::min();
  writeBigEndian(static_cast<uint64_t>(bits), out);
}

// Truncates or zero-pads the value to a fixed size, because values of different
// sizes break the interleaving.
void encodeString(StringView value, char* out) {
  std::memset(out, 0, kStringSize);
  std::memcpy(out, value.data(), std::min<size_t>(value.size(), kStringSize));
}

// Returns the number of bytes 'type' encodes to when the field is not null, or
// std::nullopt when z-ordering does not support the type.
std::optional<int32_t> encodedSize(const TypePtr& type) {
  if (*type == *TINYINT() || *type == *SMALLINT() || *type == *INTEGER() ||
      *type == *BIGINT() || *type == *REAL() || *type == *DOUBLE() ||
      *type == *BOOLEAN() || *type == *DATE() || *type == *TIMESTAMP()) {
    return kPrimitiveSize;
  }
  if (*type == *VARCHAR() || *type == *VARBINARY()) {
    return kStringSize;
  }
  return std::nullopt;
}

// Writes the order-preserving encoding of the non-null value at 'row'.
void encodeField(
    const TypePtr& type,
    const DecodedVector& field,
    vector_size_t row,
    char* out) {
  switch (type->kind()) {
    case TypeKind::TINYINT:
      return encodeWholeNumber(field.valueAt<int8_t>(row), out);
    case TypeKind::SMALLINT:
      return encodeWholeNumber(field.valueAt<int16_t>(row), out);
    case TypeKind::INTEGER:
      // Also DATE, as days since the epoch.
      return encodeWholeNumber(field.valueAt<int32_t>(row), out);
    case TypeKind::BIGINT:
      return encodeWholeNumber(field.valueAt<int64_t>(row), out);
    case TypeKind::REAL:
      // Java widens a real to a double before encoding it.
      return encodeFloatingPoint(field.valueAt<float>(row), out);
    case TypeKind::DOUBLE:
      return encodeFloatingPoint(field.valueAt<double>(row), out);
    case TypeKind::BOOLEAN:
      std::memset(out, 0, kPrimitiveSize);
      out[0] = field.valueAt<bool>(row) ? static_cast<char>(0x81) : 0;
      return;
    case TypeKind::TIMESTAMP:
      // Presto Java represents a timestamp as milliseconds since the epoch.
      return encodeWholeNumber(field.valueAt<Timestamp>(row).toMillis(), out);
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return encodeString(field.valueAt<StringView>(row), out);
    default:
      VELOX_UNREACHABLE();
  }
}

// Interleaves the bits of the field encodings, taking the next bit of each
// field in turn, starting from the highest bit. A field that has no bits left
// is skipped. Mirrors ZOrderByteUtils.interleaveBits.
void interleaveBits(
    const std::vector<std::string_view>& fields,
    int32_t size,
    char* out) {
  std::memset(out, 0, size);
  size_t sourceField = 0;
  size_t sourceByte = 0;
  int32_t sourceBit = 7;
  int32_t outputByte = 0;
  int32_t outputBit = 7;
  while (outputByte < size) {
    const auto bit =
        (static_cast<uint8_t>(fields[sourceField][sourceByte]) >> sourceBit) &
        1;
    out[outputByte] = static_cast<char>(
        static_cast<uint8_t>(out[outputByte]) | (bit << outputBit));
    if (--outputBit == -1) {
      ++outputByte;
      outputBit = 7;
    }
    if (outputByte == size) {
      break;
    }
    do {
      if (++sourceField == fields.size()) {
        sourceField = 0;
        if (--sourceBit == -1) {
          ++sourceByte;
          sourceBit = 7;
        }
      }
    } while (fields[sourceField].size() <= sourceByte);
  }
}

class ZOrderFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const auto& rowType = args[0]->type();
    VELOX_USER_CHECK(rowType->isRow(), "zorder function requires a ROW type");
    const auto numFields = rowType->size();
    std::vector<std::optional<int32_t>> fieldSizes(numFields);
    for (auto i = 0; i < numFields; ++i) {
      fieldSizes[i] = encodedSize(rowType->childAt(i));
    }

    exec::DecodedArgs decodedArgs(rows, args, context);
    const auto* decodedRow = decodedArgs.at(0);
    const auto* baseRow = decodedRow->base()->as<RowVector>();
    std::vector<DecodedVector> fields;
    fields.reserve(numFields);
    for (auto i = 0; i < numFields; ++i) {
      fields.emplace_back(*baseRow->childAt(i));
    }

    context.ensureWritable(rows, outputType, result);
    auto* flatResult = result->asFlatVector<StringView>();
    // The encoding of each field, reused across rows.
    std::vector<std::string> encodings(numFields);
    std::vector<std::string_view> encodingViews(numFields);
    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      const auto baseIndex = decodedRow->index(row);
      int32_t size = 0;
      for (auto i = 0; i < numFields; ++i) {
        // A null field encodes to zero bytes of the primitive size, even when
        // it is a string. Like Java, an unsupported type fails only on a value.
        if (fields[i].isNullAt(baseIndex)) {
          encodings[i].assign(kPrimitiveSize, '\0');
        } else {
          VELOX_USER_CHECK(
              fieldSizes[i].has_value(),
              "Cannot use column of type {} in ZOrdering, the type is "
              "unsupported. Supported types are: TINYINT, SMALLINT, INTEGER, "
              "BIGINT, REAL, DOUBLE, BOOLEAN, DATE, TIMESTAMP, VARCHAR, "
              "VARBINARY",
              rowType->childAt(i)->toString());
          encodings[i].assign(fieldSizes[i].value(), '\0');
          encodeField(
              rowType->childAt(i), fields[i], baseIndex, encodings[i].data());
        }
        encodingViews[i] = encodings[i];
        size += encodings[i].size();
      }
      exec::StringWriter writer(flatResult, row);
      writer.resize(size);
      if (size > 0) {
        interleaveBits(encodingViews, size, writer.data());
      }
      writer.finalize();
    });
  }
};

} // namespace

void registerZOrderFunction(const std::string& name) {
  exec::registerVectorFunction(
      name,
      {exec::FunctionSignatureBuilder()
           .typeVariable("T")
           .returnType("varbinary")
           .argumentType("T")
           .build()},
      std::make_unique<ZOrderFunction>());
}

} // namespace facebook::presto::iceberg::functions
