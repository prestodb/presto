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
#include "velox/expression/fuzzer/FuzzerToolkit.h"
#include "velox/vector/VectorSaver.h"

namespace facebook::velox::fuzzer {

namespace {
void saveStdVector(const std::vector<int>& list, std::ostream& out) {
  // Size of the vector
  size_t size = list.size();
  out.write((char*)&(size), sizeof(size));
  out.write(
      reinterpret_cast<const char*>(list.data()), list.size() * sizeof(int));
}

template <typename T>
std::vector<T> restoreStdVector(std::istream& in) {
  size_t size;
  in.read((char*)&size, sizeof(size));
  std::vector<T> vec(size);
  in.read(reinterpret_cast<char*>(vec.data()), size * sizeof(T));
  return vec;
}
} // namespace

std::string CallableSignature::toString() const {
  std::string buf = name;
  buf.append("( ");
  for (const auto& arg : args) {
    buf.append(arg->toString());
    buf.append(" ");
  }
  buf.append(") -> ");
  buf.append(returnType->toString());
  return buf;
}

void sortCallableSignatures(std::vector<CallableSignature>& signatures) {
  std::sort(
      signatures.begin(),
      signatures.end(),
      // Returns true if lhs is less (comes before).
      [](const CallableSignature& lhs, const CallableSignature& rhs) {
        // The comparison logic is the following:
        //
        // 1. Compare based on function name.
        // 2. If names are the same, compare the number of args.
        // 3. If number of args are the same, look for any different arg
        // types.
        // 4. If all arg types are the same, compare return type.
        if (lhs.name == rhs.name) {
          if (lhs.args.size() == rhs.args.size()) {
            for (size_t i = 0; i < lhs.args.size(); ++i) {
              if (!lhs.args[i]->kindEquals(rhs.args[i])) {
                return lhs.args[i]->toString() < rhs.args[i]->toString();
              }
            }

            return lhs.returnType->toString() < rhs.returnType->toString();
          }
          return lhs.args.size() < rhs.args.size();
        }
        return lhs.name < rhs.name;
      });
}

void sortSignatureTemplates(std::vector<SignatureTemplate>& signatures) {
  std::sort(
      signatures.begin(),
      signatures.end(),
      // Returns true if lhs is less (comes before).
      [](const SignatureTemplate& lhs, const SignatureTemplate& rhs) {
        // The comparison logic is the following:
        //
        // 1. Compare based on function name.
        // 2. If names are the same, compare the number of args.
        // 3. If number of args are the same, look for any different arg
        // types.
        // 4. If all arg types are the same, compare return type.
        if (lhs.name == rhs.name) {
          auto& leftArgs = lhs.signature->argumentTypes();
          auto& rightArgs = rhs.signature->argumentTypes();
          if (leftArgs.size() == rightArgs.size()) {
            for (size_t i = 0; i < leftArgs.size(); ++i) {
              if (!(leftArgs[i] == rightArgs[i])) {
                return leftArgs[i].toString() < rightArgs[i].toString();
              }
            }
          }
          return leftArgs.size() < rightArgs.size();
        }
        return lhs.name < rhs.name;
      });
}

void compareExceptions(
    std::exception_ptr commonPtr,
    std::exception_ptr simplifiedPtr) {
  // If we don't have two exceptions, fail.
  if (!commonPtr || !simplifiedPtr) {
    LOG(ERROR) << "Only " << (commonPtr ? "common" : "simplified")
               << " path threw exception:";
    if (commonPtr) {
      std::rethrow_exception(commonPtr);
    } else {
      std::rethrow_exception(simplifiedPtr);
    }
  }
  LOG(INFO) << "Exceptions match.";
}

void compareVectors(
    const VectorPtr& left,
    const VectorPtr& right,
    const std::string& leftName,
    const std::string& rightName,
    const std::optional<SelectivityVector>& rowsInput) {
  // Print vector contents if in verbose mode.
  VLOG(1) << "Comparing vectors " << leftName << " vs " << rightName;

  if (!rowsInput.has_value()) {
    VELOX_CHECK_EQ(left->size(), right->size(), "Vectors must be equal size.");
  }

  const auto& rows =
      rowsInput.has_value() ? *rowsInput : SelectivityVector(left->size());

  rows.applyToSelected([&](vector_size_t row) {
    VLOG(1) << fmt::format(
        "At {} [ {} vs {} ]", row, left->toString(row), right->toString(row));
  });
  VLOG(1) << "===================";

  rows.applyToSelected([&](vector_size_t row) {
    VELOX_CHECK(
        left->equalValueAt(right.get(), row, row),
        "Different values at idx '{}': '{}' vs. '{}'",
        row,
        left->toString(row),
        right->toString(row));
  });

  LOG(INFO) << "Two vectors match.";
}

RowVectorPtr mergeRowVectors(
    const std::vector<RowVectorPtr>& results,
    velox::memory::MemoryPool* pool) {
  auto totalCount = 0;
  for (const auto& result : results) {
    totalCount += result->size();
  }
  auto copy =
      BaseVector::create<RowVector>(results[0]->type(), totalCount, pool);
  auto copyCount = 0;
  for (const auto& result : results) {
    copy->copy(result.get(), copyCount, 0, result->size());
    copyCount += result->size();
  }
  return copy;
}

void InputRowMetadata::saveToFile(const char* filePath) const {
  std::ofstream outputFile(filePath, std::ofstream::binary);
  saveStdVector(columnsToWrapInLazy, outputFile);
  saveStdVector(columnsToWrapInCommonDictionary, outputFile);
  outputFile.close();
}

InputRowMetadata InputRowMetadata::restoreFromFile(
    const char* filePath,
    memory::MemoryPool* pool) {
  InputRowMetadata ret;
  std::ifstream in(filePath, std::ifstream::binary);
  ret.columnsToWrapInLazy = restoreStdVector<int>(in);
  if (in.peek() != EOF) {
    // this check allows reading old files that only saved columnsToWrapInLazy.
    ret.columnsToWrapInCommonDictionary = restoreStdVector<int>(in);
  }
  in.close();
  return ret;
}

void ExprBank::insert(const core::TypedExprPtr& expression) {
  auto typeString = expression->type()->toString();
  if (typeToExprsByLevel_.find(typeString) == typeToExprsByLevel_.end()) {
    typeToExprsByLevel_.insert(
        {typeString, ExprsIndexedByLevel(maxLevelOfNesting_ + 1)});
  }
  auto& expressionsByLevel = typeToExprsByLevel_[typeString];
  int nestingLevel = getNestedLevel(expression);
  VELOX_CHECK_LE(nestingLevel, maxLevelOfNesting_);
  expressionsByLevel[nestingLevel].push_back(expression);
}

core::TypedExprPtr ExprBank::getRandomExpression(
    const facebook::velox::TypePtr& returnType,
    int uptoLevelOfNesting) {
  VELOX_CHECK_LE(uptoLevelOfNesting, maxLevelOfNesting_);
  auto typeString = returnType->toString();
  if (typeToExprsByLevel_.find(typeString) == typeToExprsByLevel_.end()) {
    return nullptr;
  }
  auto& expressionsByLevel = typeToExprsByLevel_[typeString];
  int totalToConsider = 0;
  for (int i = 0; i <= uptoLevelOfNesting; i++) {
    totalToConsider += expressionsByLevel[i].size();
  }
  if (totalToConsider > 0) {
    int choice = boost::random::uniform_int_distribution<uint32_t>(
        0, totalToConsider - 1)(rng_);
    for (int i = 0; i <= uptoLevelOfNesting; i++) {
      if (choice >= expressionsByLevel[i].size()) {
        choice -= expressionsByLevel[i].size();
        continue;
      }
      return expressionsByLevel[i][choice];
    }
    VELOX_CHECK(false, "Should have found an expression.");
  }
  return nullptr;
}

} // namespace facebook::velox::fuzzer
