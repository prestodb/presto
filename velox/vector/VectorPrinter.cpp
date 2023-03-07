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
#include "velox/vector/VectorPrinter.h"
#include <sstream>
#include "velox/vector/ComplexVector.h"

namespace facebook::velox {

namespace {

const std::string kIndent = "   ";

std::string addIndent(const std::string& indent) {
  return indent + kIndent;
}

std::string printFixedWidth(
    const DecodedVector& decodedVector,
    vector_size_t index) {
  if (decodedVector.isNullAt(index)) {
    return "<null>";
  }

  auto base = decodedVector.base();
  auto baseIndex = decodedVector.index(index);
  return base->toString(baseIndex);
}

class VectorPrinter {
 public:
  explicit VectorPrinter(const BaseVector& vector) : decoded_{vector} {}

  virtual ~VectorPrinter() = default;

  std::string summarize(vector_size_t index) const {
    if (decoded_.isNullAt(index)) {
      return fmt::format("{} <null>", decoded_.base()->type()->toString());
    }

    return summarizeNonNull(index);
  }

  std::string print(vector_size_t index, const std::string& indent) const {
    if (decoded_.isNullAt(index)) {
      std::ostringstream out;
      out << indent << "<null>" << std::endl;
      return out.str();
    }

    return printNonNull(index, indent);
  }

  const DecodedVector& decoded() const {
    return decoded_;
  }

 protected:
  virtual std::string printNonNull(
      vector_size_t index,
      const std::string& indent) const = 0;

  virtual std::string summarizeNonNull(vector_size_t index) const = 0;

  DecodedVector decoded_;
  std::vector<std::unique_ptr<VectorPrinter>> children_;
};

std::unique_ptr<VectorPrinter> createVectorPrinter(const BaseVector& vector);

class PrimitiveVectorPrinter : public VectorPrinter {
 public:
  explicit PrimitiveVectorPrinter(const BaseVector& vector)
      : VectorPrinter(vector) {}

 protected:
  std::string printNonNull(vector_size_t index, const std::string& indent)
      const override {
    std::stringstream out;

    out << indent << decoded_.base()->toString(decoded_.index(index))
        << std::endl;
    return out.str();
  }

  std::string summarizeNonNull(vector_size_t index) const override {
    auto* base = decoded_.base();
    if (base->typeKind() == TypeKind::VARCHAR ||
        base->typeKind() == TypeKind::VARBINARY) {
      return fmt::format(
          "{} size: {}",
          base->type()->toString(),
          decoded_.valueAt<StringView>(index).size());
    } else {
      return base->type()->toString();
    }
  }
};

class ArrayVectorPrinter : public VectorPrinter {
 public:
  explicit ArrayVectorPrinter(const BaseVector& vector)
      : VectorPrinter(vector) {
    auto* arrayVector = decoded_.base()->as<ArrayVector>();
    children_.emplace_back(createVectorPrinter(*arrayVector->elements()));
  }

 protected:
  std::string printNonNull(vector_size_t index, const std::string& indent)
      const override {
    std::stringstream out;

    auto arrayVector = decoded_.base()->as<ArrayVector>();
    auto arrayIndex = decoded_.index(index);

    const auto& elements = children_[0];

    auto offset = arrayVector->offsetAt(arrayIndex);
    auto size = arrayVector->sizeAt(arrayIndex);

    auto newIndent = addIndent(indent);
    bool fixedWidthElement = arrayVector->type()->childAt(0)->isFixedWidth();

    for (auto i = 0; i < size; ++i) {
      if (fixedWidthElement) {
        out << indent << "Element " << i << ": "
            << printFixedWidth(elements->decoded(), offset + i) << std::endl;
      } else {
        out << indent << "Element " << i << ": "
            << elements->summarize(offset + i) << std::endl;
        out << elements->print(offset + i, newIndent);
      }
    }

    return out.str();
  }

  std::string summarizeNonNull(vector_size_t index) const override {
    auto* base = decoded_.base();
    auto baseIndex = decoded_.index(index);
    return fmt::format(
        "{} size: {}",
        base->type()->toString(),
        base->as<ArrayVector>()->sizeAt(baseIndex));
  }
};

class MapVectorPrinter : public VectorPrinter {
 public:
  explicit MapVectorPrinter(const BaseVector& vector) : VectorPrinter(vector) {
    auto* mapVector = decoded_.base()->as<MapVector>();
    children_.emplace_back(createVectorPrinter(*mapVector->mapKeys()));
    children_.emplace_back(createVectorPrinter(*mapVector->mapValues()));
  }

 protected:
  std::string printNonNull(vector_size_t index, const std::string& indent)
      const override {
    std::stringstream out;

    auto mapVector = decoded_.base()->as<MapVector>();
    auto mapIndex = decoded_.index(index);

    const auto& keys = children_[0];
    const auto& values = children_[1];

    auto offset = mapVector->offsetAt(mapIndex);
    auto size = mapVector->sizeAt(mapIndex);

    auto newIndent = addIndent(indent);

    bool fixedWidthKey = mapVector->type()->childAt(0)->isFixedWidth();
    bool fixedWidthValue = mapVector->type()->childAt(1)->isFixedWidth();

    for (auto i = 0; i < size; ++i) {
      if (fixedWidthKey) {
        out << indent << "Key " << i << ": "
            << printFixedWidth(keys->decoded(), offset + 1) << std::endl;
      } else {
        out << indent << "Key " << i << ": " << values->summarize(offset + i)
            << std::endl;
        out << keys->print(offset + i, newIndent);
      }

      if (fixedWidthValue) {
        out << indent << "Value " << i << ": "
            << printFixedWidth(values->decoded(), offset + i) << std::endl;
      } else {
        out << indent << "Value " << i << ": " << values->summarize(offset + i)
            << std::endl;
        out << values->print(offset + i, newIndent);
      }
    }

    return out.str();
  }

  std::string summarizeNonNull(vector_size_t index) const override {
    auto* base = decoded_.base();
    auto baseIndex = decoded_.index(index);
    return fmt::format(
        "{} size: {}",
        base->type()->toString(),
        base->as<MapVector>()->sizeAt(baseIndex));
  }
};

class RowVectorPrinter : public VectorPrinter {
 public:
  explicit RowVectorPrinter(const BaseVector& vector) : VectorPrinter(vector) {
    auto* rowVector = decoded_.base()->as<RowVector>();
    for (const auto& child : rowVector->children()) {
      children_.emplace_back(createVectorPrinter(*child));
    }
  }

 protected:
  std::string printNonNull(vector_size_t index, const std::string& indent)
      const override {
    std::stringstream out;

    auto rowIndex = decoded_.index(index);

    const auto& rowType = decoded_.base()->type()->asRow();

    for (auto i = 0; i < rowType.size(); ++i) {
      out << indent << "Field " << rowType.nameOf(i) << ": "
          << children_[i]->summarize(rowIndex) << std::endl;
      out << children_[i]->print(rowIndex, addIndent(indent));
    }

    return out.str();
  }

  std::string summarizeNonNull(vector_size_t /* index */) const override {
    return decoded_.base()->type()->toString();
  }
};

std::unique_ptr<VectorPrinter> createVectorPrinter(const BaseVector& vector) {
  switch (vector.typeKind()) {
    case TypeKind::ARRAY:
      return std::make_unique<ArrayVectorPrinter>(vector);
    case TypeKind::MAP:
      return std::make_unique<MapVectorPrinter>(vector);
    case TypeKind::ROW:
      return std::make_unique<RowVectorPrinter>(vector);
    default:
      return std::make_unique<PrimitiveVectorPrinter>(vector);
  }
}

void printSizeAndNullCount(const BaseVector& vector, std::ostringstream& out) {
  out << vector.size() << " elements, ";
  vector_size_t nullCount = 0;
  if (vector.isConstantEncoding()) {
    // Nulls buffer for constant vector contains at most 1 entry.
    if (vector.isNullAt(0)) {
      nullCount = vector.size();
    }
  } else {
    nullCount = BaseVector::countNulls(vector.nulls(), vector.size());
  }

  if (nullCount > 0) {
    out << nullCount << " nulls";
  } else {
    out << "no nulls";
  }
}

void printEncodingAndType(
    const BaseVector& vector,
    const std::string& indent,
    std::ostringstream& out) {
  out << indent << VectorEncoding::mapSimpleToName(vector.encoding()) << ": "
      << vector.type()->toString() << " ";
  printSizeAndNullCount(vector, out);
  out << std::endl;
}

std::string printTypeAndEncodingTree(
    const BaseVector& vector,
    const std::string& indent) {
  std::ostringstream out;

  const auto newIndent = addIndent(indent);
  switch (vector.encoding()) {
    case VectorEncoding::Simple::CONSTANT:
    case VectorEncoding::Simple::DICTIONARY: {
      out << indent << VectorEncoding::mapSimpleToName(vector.encoding())
          << " ";
      printSizeAndNullCount(vector, out);
      out << std::endl;
      // Constant vector of primitive type doesn't have valueVector.
      if (vector.valueVector()) {
        out << printTypeAndEncodingTree(*vector.valueVector(), newIndent);
      } else {
        out << newIndent << vector.type()->toString() << std::endl;
      }
      break;
    }
    case VectorEncoding::Simple::FLAT:
      printEncodingAndType(vector, indent, out);
      break;
    case VectorEncoding::Simple::ARRAY: {
      auto* arrayVector = vector.as<ArrayVector>();
      printEncodingAndType(vector, indent, out);
      out << indent << "Elements: " << std::endl;
      out << printTypeAndEncodingTree(*arrayVector->elements(), newIndent);
      break;
    }
    case VectorEncoding::Simple::MAP: {
      auto* mapVector = vector.as<MapVector>();
      printEncodingAndType(vector, indent, out);
      out << indent << "Keys: " << std::endl;
      out << printTypeAndEncodingTree(*mapVector->mapKeys(), newIndent);
      out << indent << "Values: " << std::endl;
      out << printTypeAndEncodingTree(*mapVector->mapValues(), newIndent);
      break;
    }
    case VectorEncoding::Simple::ROW: {
      printEncodingAndType(vector, indent, out);
      const auto* rowVector = vector.as<RowVector>();
      const auto& rowType = vector.type()->asRow();
      for (auto i = 0; i < rowType.size(); ++i) {
        out << indent << "Field " << rowType.nameOf(i) << ":" << std::endl;
        out << printTypeAndEncodingTree(*rowVector->childAt(i), newIndent);
      }
      break;
    }
    default:
      VELOX_UNSUPPORTED(
          "Unsupported encoding: {}",
          VectorEncoding::mapSimpleToName(vector.encoding()));
  }

  return out.str();
}
} // namespace

std::string printVector(const BaseVector& vector) {
  SelectivityVector rows(vector.size());
  return printVector(vector, rows);
}

std::string
printVector(const BaseVector& vector, vector_size_t from, vector_size_t size) {
  VELOX_CHECK_GE(from, 0);
  VELOX_CHECK_GE(size, 0);

  auto end = std::min(from + size, vector.size());

  SelectivityVector rows(end, false);

  rows.setValidRange(from, end, true);
  rows.updateBounds();

  return printVector(vector, rows);
}

std::string printVector(
    const BaseVector& vector,
    const SelectivityVector& rows) {
  auto printer = createVectorPrinter(vector);

  static const std::string kSeparator(120, '-');

  std::stringstream out;

  out << vector.toString() << std::endl;
  out << kSeparator << std::endl;

  out << printTypeAndEncodingTree(vector, "");
  out << kSeparator << std::endl;

  rows.applyToSelected([&](auto i) {
    out << "Row " << i << ": " << printer->summarize(i) << std::endl;
    out << printer->print(i, kIndent);
    out << kSeparator << std::endl;
  });

  return out.str();
}

} // namespace facebook::velox
