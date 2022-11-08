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
#include "velox/expression/tests/FuzzerToolkit.h"

namespace facebook::velox::test {

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

namespace {

bool isInvalidArgumentOrUnsupported(const VeloxException& ex) {
  return ex.errorCode() == "INVALID_ARGUMENT" ||
      ex.errorCode() == "UNSUPPORTED";
}

void compareExceptions(
    const VeloxException& left,
    const VeloxException& right) {
  // Error messages sometimes differ; check at least error codes.
  // Since the common path may peel the input encoding off, whereas the
  // simplified path flatten input vectors, the common and the simplified
  // paths may evaluate the input rows in different orders and hence throw
  // different exceptions depending on which bad input they come across
  // first. We have seen this happen for the format_datetime Presto function
  // that leads to unmatched error codes UNSUPPORTED vs. INVALID_ARGUMENT.
  // Therefore, we intentionally relax the comparision here.
  if (left.errorCode() == right.errorCode() ||
      (isInvalidArgumentOrUnsupported(left) &&
       isInvalidArgumentOrUnsupported(right))) {
    VELOX_CHECK_EQ(left.errorSource(), right.errorSource());
    VELOX_CHECK_EQ(left.exceptionName(), right.exceptionName());
    if (left.message() != right.message()) {
      LOG(WARNING) << "Two different VeloxExceptions were thrown:\n\t"
                   << left.message() << "\nand\n\t" << right.message();
    }
  } else {
    LOG(ERROR) << left.what();
    LOG(ERROR) << right.what();
    VELOX_FAIL(
        "Two different VeloxExceptions were thrown: {} vs. {}",
        left.message(),
        right.message());
  }
}
} // namespace

void compareExceptions(
    std::exception_ptr commonPtr,
    std::exception_ptr simplifiedPtr) {
  // If we don't have two exceptions, fail.
  if (!commonPtr || !simplifiedPtr) {
    LOG(ERROR) << "Only one path threw exception:";
    if (commonPtr) {
      std::rethrow_exception(commonPtr);
    } else {
      std::rethrow_exception(simplifiedPtr);
    }
  }

  // Otherwise, make sure the exceptions are the same.
  try {
    std::rethrow_exception(commonPtr);
  } catch (const VeloxException& ve1) {
    try {
      std::rethrow_exception(simplifiedPtr);
    } catch (const VeloxException& ve2) {
      compareExceptions(ve1, ve2);
      return;
    } catch (const std::exception& e2) {
      LOG(WARNING) << "Two different exceptions were thrown:\n\t"
                   << ve1.message() << "\nand\n\t" << e2.what();
    }
  } catch (const std::exception& e1) {
    try {
      std::rethrow_exception(simplifiedPtr);
    } catch (const std::exception& e2) {
      if (e1.what() != e2.what()) {
        LOG(WARNING) << "Two different std::exceptions were thrown:\n\t"
                     << e1.what() << "\nand\n\t" << e2.what();
      }
      return;
    }
  }
  VELOX_FAIL("Got two incompatible exceptions.");
}

} // namespace facebook::velox::test
