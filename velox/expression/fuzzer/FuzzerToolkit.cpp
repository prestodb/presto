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

namespace facebook::velox::fuzzer {

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

} // namespace facebook::velox::fuzzer
