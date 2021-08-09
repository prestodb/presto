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
#pragma once

#include "velox/core/Expressions.h"
#include "velox/core/ITypedExpr.h"
#include "velox/experimental/codegen/vector_function/GeneratedVectorFunction-inl.h"

namespace facebook {
namespace velox {
namespace codegen {

/// Models compiled expressions.
/// In general, generation function from a dynamicLib produce multiple output.
/// A compiled expression is represented by an dynamicLib
/// And an index indicating which output of the loaded function
/// this object represent
class ICompiledCall : public core::CallTypedExpr {
 public:
  using NewInstanceSignature =
      std::unique_ptr<GeneratedVectorFunctionBase> (*)();

  ICompiledCall(
      const std::filesystem::path& dynamicLibPath,
      const std::vector<std::shared_ptr<const ITypedExpr>>& inputs,
      const std::shared_ptr<const RowType>& rowType)
      : core::CallTypedExpr(rowType, inputs, ""),
        dynamicLibPath_{dynamicLibPath} {}
  ICompiledCall(const ICompiledCall&) = delete;
  ICompiledCall(ICompiledCall&&) = delete;

  virtual std::string toString() const override {
    return "[Compiled] : " + core::CallTypedExpr::toString();
  };

  // TODO: Missing tests
  virtual bool operator==(const ITypedExpr& other) const override {
    if (auto otherCall = dynamic_cast<const ICompiledCall*>(&other);
        otherCall != nullptr) {
      return otherCall == this;
    };
    return false;
  };

  // The compiled function is registered right before
  // we return its name. This allow for lazy initialization.
  virtual const std::string& name() const override {
    if (!name_.has_value()) {
      name_ = registerFunction();
    };

    VELOX_CHECK_NOT_NULL(exec::getVectorFunction(name_.value(), {}, {}));
    return name_.value();
  }

  std::unique_ptr<GeneratedVectorFunctionBase> newInstance() const {
    if (!newInstanceFunction_.has_value()) {
      auto& loader = native_loader::NativeLibraryLoader::getDefaultLoader();
      auto loadedLibrary = loader.loadLibrary(dynamicLibPath_, nullptr);
      auto newInstance = loader.getFunction<NewInstanceSignature>(
          "newInstance", loadedLibrary);
      newInstanceFunction_ = newInstance;
    }
    auto generatedVectorFunction = newInstanceFunction_.value()();
    return generatedVectorFunction;
  }

 private:
  std::string registerFunction() const {
    auto generatedVectorFunction = newInstance();
    VELOX_CHECK_NOT_NULL(
        std::dynamic_pointer_cast<const RowType>(this->type()));
    generatedVectorFunction->setRowType(
        std::dynamic_pointer_cast<const RowType>(this->type()));

    return insertFunction(std::move(generatedVectorFunction));
  }

  // Idealy we should push this code closer to vectorFunction.cpp
  // In particular, we should had an "annonymous" registration where the map
  // it self chose a random name, instead of us guessing.
  std::string insertFunction(std::unique_ptr<GeneratedVectorFunctionBase>
                                 generatedVectorFunction) const {
    const char* compiledFunctionNameFormat = "compiledFunction_{}";
    static const size_t kMaxRegistrationTry = 100;
    static const size_t kMaxRegisteredFunction = 10000;
    // Seed with a real random value, if available
    std::random_device r;
    std::uniform_int_distribution<int> uniform_dist(1, kMaxRegisteredFunction);
    int functionID = uniform_dist(r);
    std::string functionName =
        fmt::format(compiledFunctionNameFormat, functionID);
    size_t tryCounter = 0;
    while (!exec::registerVectorFunction(
        functionName, {}, std::move(generatedVectorFunction))) {
      if (tryCounter > kMaxRegistrationTry) {
        throw std::runtime_error(fmt::format(
            "Can't register new function after {} attempts", tryCounter));
      }
      functionID = uniform_dist(r);
      functionName = fmt::format(compiledFunctionNameFormat, functionID);
      tryCounter++;
    };
    return functionName;
  };

  std::filesystem::path dynamicLibPath_;
  mutable std::optional<std::string> name_;
  mutable std::optional<NewInstanceSignature> newInstanceFunction_;
};
} // namespace codegen
} // namespace velox
} // namespace facebook
