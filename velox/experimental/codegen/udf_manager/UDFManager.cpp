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

#include "velox/experimental/codegen/udf_manager/UDFManager.h"
namespace facebook::velox {
namespace codegen {

void registerVeloxArithmeticUDFs(UDFManager& udfManager) {
  const std::vector<std::string> arithmeticHeaders = {
      "\"velox/functions/prestosql/CheckedArithmeticImpl.h\"",
      "\"velox/functions/prestosql/ArithmeticImpl.h\""};

  auto getDefaultArithmeticUDF = [&]() {
    return UDFInformation()
        .withHeaderFiles(arithmeticHeaders)
        .withNullMode(ExpressionNullMode::NullInNullOut)
        .withIsOptionalArguments(false)
        .withIsOptionalOutput(false)
        .withOutputForm(OutputForm::Return)
        .withCodegenNullInNullOutChecks(true);
  };

  // Helper function to register arithmetic UDF.
  auto registerUDF = [&](const std::unordered_set<std::string>& veloxNames,
                         const std::string& calledFunctionName,
                         const std::vector<velox::TypeKind>& argumentsTypes) {
    udfManager.registerUDF(getDefaultArithmeticUDF()
                               .withVeloxFunctionNames(veloxNames)
                               .withCalledFunctionName(fmt::format(
                                   "functions::{}", calledFunctionName))
                               .withArgumentsLookupTypes(argumentsTypes));
  };

  // Matches registerBinaryIntegral in Velox.
  auto registerBinaryIntegralUDF = [&](const std::unordered_set<std::string>&
                                           veloxNames,
                                       const std::string& calledFunctionName) {
    registerUDF(
        veloxNames, calledFunctionName, {TypeKind::BIGINT, TypeKind::BIGINT});

    registerUDF(
        veloxNames, calledFunctionName, {TypeKind::INTEGER, TypeKind::INTEGER});
  };

  // Matches registerUnaryIntegral in Velox.
  auto registerUnaryIntegralUDF =
      [&](const std::unordered_set<std::string>& veloxNames,
          const std::string& calledFunctionName) {
        registerUDF(veloxNames, calledFunctionName, {TypeKind::BIGINT});
        registerUDF(veloxNames, calledFunctionName, {TypeKind::INTEGER});
        registerUDF(veloxNames, calledFunctionName, {TypeKind::SMALLINT});
        registerUDF(veloxNames, calledFunctionName, {TypeKind::TINYINT});
      };

  // Matches registerBinaryFloatingPoint in Velox.
  auto registerBinaryFloatingPointUDF =
      [&](const std::unordered_set<std::string>& veloxNames,
          const std::string& calledFunctionName) {
        registerUDF(
            veloxNames, calledFunctionName, {TypeKind::REAL, TypeKind::REAL});
        registerUDF(
            veloxNames,
            calledFunctionName,
            {TypeKind::DOUBLE, TypeKind::DOUBLE});
      };

  // Matches registerUnaryFloatingPointUDF in Velox.
  auto registerUnaryFloatingPointUDF =
      [&](const std::unordered_set<std::string>& veloxNames,
          const std::string& calledFunctionName) {
        registerUDF(veloxNames, calledFunctionName, {TypeKind::REAL});
        registerUDF(veloxNames, calledFunctionName, {TypeKind::DOUBLE});
      };

  // Matches registerUnaryNumeric in Velox.
  auto registerUnaryNumeric =
      [&](const std::unordered_set<std::string>& veloxNames,
          const std::string& calledFunctionName) {
        registerUnaryFloatingPointUDF(veloxNames, calledFunctionName);
        registerUnaryIntegralUDF(veloxNames, calledFunctionName);
      };

  registerBinaryIntegralUDF({"plus"}, "checkedPlus");
  registerBinaryFloatingPointUDF({"plus"}, "plus");

  registerBinaryIntegralUDF({"minus"}, "checkedMinus");
  registerBinaryFloatingPointUDF({"minus"}, "minus");

  registerBinaryIntegralUDF({"multiply"}, "checkedMultiply");
  registerBinaryFloatingPointUDF({"multiply"}, "multiply");

  registerBinaryIntegralUDF({"divide"}, "checkedDivide");
  registerBinaryFloatingPointUDF({"divide"}, "divide");

  registerUnaryIntegralUDF({"negate"}, "checkedNegate");
  registerUnaryFloatingPointUDF({"negate"}, "negate");

  registerBinaryIntegralUDF({"modulus"}, "checkedModulus");

  registerUnaryNumeric({"abs"}, "abs");
  registerUnaryNumeric({"floor"}, "floor");
  registerUnaryNumeric({"ceil", "ceiling"}, "ceil");

  // Round function
  UDFInformation roundUDF = getDefaultArithmeticUDF()
                                .withVeloxFunctionNames({"round"})
                                .withCalledFunctionName("functions::round");

  udfManager.registerUDF(roundUDF.withArgumentsLookupTypes(
      {TypeKind::INTEGER, TypeKind::INTEGER}));

  udfManager.registerUDF(
      roundUDF.withArgumentsLookupTypes({TypeKind::BIGINT, TypeKind::INTEGER}));

  udfManager.registerUDF(roundUDF.withArgumentsLookupTypes(
      {TypeKind::SMALLINT, TypeKind::INTEGER}));

  udfManager.registerUDF(roundUDF.withArgumentsLookupTypes(
      {TypeKind::TINYINT, TypeKind::INTEGER}));

  udfManager.registerUDF(roundUDF.withArgumentsLookupTypes(
      {TypeKind::TINYINT, TypeKind::INTEGER}));

  udfManager.registerUDF(
      roundUDF.withArgumentsLookupTypes({TypeKind::DOUBLE, TypeKind::INTEGER}));

  udfManager.registerUDF(
      roundUDF.withArgumentsLookupTypes({TypeKind::REAL, TypeKind::INTEGER}));

  // Second argument is optional
  udfManager.registerUDF(
      roundUDF.withArgumentsLookupTypes({TypeKind::INTEGER}));

  udfManager.registerUDF(roundUDF.withArgumentsLookupTypes({TypeKind::BIGINT}));

  udfManager.registerUDF(
      roundUDF.withArgumentsLookupTypes({TypeKind::SMALLINT}));

  udfManager.registerUDF(
      roundUDF.withArgumentsLookupTypes({TypeKind::TINYINT}));

  udfManager.registerUDF(
      roundUDF.withArgumentsLookupTypes({TypeKind::TINYINT}));

  udfManager.registerUDF(roundUDF.withArgumentsLookupTypes({TypeKind::DOUBLE}));

  udfManager.registerUDF(roundUDF.withArgumentsLookupTypes({TypeKind::REAL}));

  // Register hash function
  UDFInformation hashBaseUDF =
      getDefaultArithmeticUDF()
          .withHeaderFiles(
              {"\"velox/experimental/codegen/functions/HashFunctionStub.h\""})
          .withVeloxFunctionNames({"hash"})
          .withCalledFunctionName("codegen::computeHashStub");

  udfManager.registerUDF(
      hashBaseUDF.withArgumentsLookupTypes({TypeKind::INTEGER}));
  udfManager.registerUDF(
      hashBaseUDF.withArgumentsLookupTypes({TypeKind::BIGINT}));
  udfManager.registerUDF(
      hashBaseUDF.withArgumentsLookupTypes({TypeKind::SMALLINT}));
  udfManager.registerUDF(
      hashBaseUDF.withArgumentsLookupTypes({TypeKind::TINYINT}));
  udfManager.registerUDF(
      hashBaseUDF.withArgumentsLookupTypes({TypeKind::DOUBLE}));
  udfManager.registerUDF(
      hashBaseUDF.withArgumentsLookupTypes({TypeKind::REAL}));
  udfManager.registerUDF(
      hashBaseUDF.withArgumentsLookupTypes({TypeKind::VARCHAR}));

  // Between is added with arithmetic for now since its the only non-built in
  // logical
  UDFInformation betweenBaseUDF =
      getDefaultArithmeticUDF()
          .withHeaderFiles({"\"velox/functions_core/ComparisonsImpl.h\""})
          .withVeloxFunctionNames({"between"})
          .withCalledFunctionName("functions::between");

  udfManager.registerUDF(betweenBaseUDF.withArgumentsLookupTypes(
      {TypeKind::INTEGER, TypeKind::INTEGER, TypeKind::INTEGER}));
  udfManager.registerUDF(betweenBaseUDF.withArgumentsLookupTypes(
      {TypeKind::BIGINT, TypeKind::BIGINT, TypeKind::BIGINT}));
  udfManager.registerUDF(betweenBaseUDF.withArgumentsLookupTypes(
      {TypeKind::SMALLINT, TypeKind::SMALLINT, TypeKind::SMALLINT}));
  udfManager.registerUDF(betweenBaseUDF.withArgumentsLookupTypes(
      {TypeKind::TINYINT, TypeKind::TINYINT, TypeKind::TINYINT}));

  udfManager.registerUDF(betweenBaseUDF.withArgumentsLookupTypes(
      {TypeKind::DOUBLE, TypeKind::DOUBLE, TypeKind::DOUBLE}));
  udfManager.registerUDF(betweenBaseUDF.withArgumentsLookupTypes(
      {TypeKind::REAL, TypeKind::REAL, TypeKind::REAL}));

  // Add rand udf
  UDFInformation randUDF =
      getDefaultArithmeticUDF()
          .withCalledFunctionName(" folly::Random::randDouble01")
          .withVeloxFunctionNames({"rand"})
          .withHeaderFiles({"\"folly/Random.h\""});

  udfManager.registerUDF(randUDF.withArgumentsLookupTypes({}));
}

UDFInformation getCastUDFBase() {
  const std::vector<std::string> headers = {
      "\"velox/experimental/codegen/functions/CastFunctionStub.h\""};

  return UDFInformation()
      .withHeaderFiles(headers)
      .withNullMode(ExpressionNullMode::NullInNullOut)
      .withIsOptionalArguments(false)
      .withIsOptionalOutput(false)
      .withOutputForm(OutputForm::Return)
      .withCodegenNullInNullOutChecks(true);
};

UDFInformation getCastUDF(velox::TypeKind kind, bool castByTruncate) {
  auto functionName = fmt::format(
      "codegen::CodegenConversionStub<{toKind}, {truncate}>::cast",
      fmt::arg("toKind", fmt::format("TypeKind::{}", mapTypeKindToName(kind))),
      fmt::arg("truncate", castByTruncate ? "true" : "false"));

  return getCastUDFBase().withCalledFunctionName(functionName);
}

void registerVeloxStringFunctions(UDFManager& udfManager) {
  // Create a basic cast UDF missing velox name and the called name
  auto getStringUDFBase = []() {
    const std::vector<std::string> headers = {
        "\"velox/functions/lib/string/StringImpl.h\""};

    return UDFInformation()
        .withHeaderFiles(headers)
        .withNullMode(ExpressionNullMode::NullInNullOut)
        .withIsOptionalArguments(false)
        .withIsOptionalOutput(false)
        .withOutputForm(OutputForm::InOut)
        .withCodegenNullInNullOutChecks(true);
  };

  // Those will dynamically scan the encoding and do the fast path if needed
  udfManager.registerUDF(
      getStringUDFBase().withVeloxFunctionNames({"upper"}).withCalledFunctionName(
          "functions::stringImpl::upper<functions::stringCore::StringEncodingMode::MOSTLY_ASCII>"));

  udfManager.registerUDF(
      getStringUDFBase().withVeloxFunctionNames({"lower"}).withCalledFunctionName(
          "functions::stringImpl::lower<functions::stringCore::StringEncodingMode::MOSTLY_ASCII>"));

  // This concat does not do the optimization at which the children are
  // written directly in the output.
  udfManager.registerUDF(
      getStringUDFBase()
          .withVeloxFunctionNames({"concat"})
          .withCalledFunctionName("functions::stringImpl::concatStatic"));

  // Not optimized for ASCII
  udfManager.registerUDF(
      getStringUDFBase()
          .withOutputForm(OutputForm::Return)
          .withArgumentsLookupTypes({TypeKind::VARCHAR})
          .withVeloxFunctionNames({"length"})
          .withCalledFunctionName(
              "functions::stringImpl::length<functions::stringCore::StringEncodingMode::MOSTLY_ASCII>"));
}

void UDFManager::registerUDF(const UDFInformation& udfInformation) {
  udfInformation.validate();
  for (auto& name : udfInformation.getVeloxFunctionNames()) {
    if (udfInformation.lookUpByName()) {
      std::unique_lock lock(udfDictionaryNameOnlyMutex);
      udfDictionaryNameOnly.emplace(name, udfInformation);
    } else {
      std::unique_lock lock(udfDictionaryWithArgumentsMutex);
      udfDictionaryWithArguments.emplace(
          std::make_pair(name, udfInformation.getArgumentsLookupTypes()),
          udfInformation);
    }
  }
}

std::optional<UDFInformation> UDFManager::getUDFInformationTypedArgs(
    const std::string& functionName,
    const std::vector<velox::TypeKind>& argumentTypes) const {
  std::shared_lock lock(udfDictionaryWithArgumentsMutex);

  auto key = std::make_pair(functionName, argumentTypes);

  auto udfIterator = udfDictionaryWithArguments.find(key);
  return udfIterator == udfDictionaryWithArguments.end()
      ? std::nullopt
      : std::optional<UDFInformation>{udfIterator->second};
}

std::optional<UDFInformation> UDFManager::getUDFInformationUnTypedArgs(
    const std::string& functionName) const {
  std::shared_lock lock(udfDictionaryNameOnlyMutex);

  auto udfIterator = udfDictionaryNameOnly.find(functionName);
  return udfIterator == udfDictionaryNameOnly.end()
      ? std::nullopt
      : std::optional<UDFInformation>{udfIterator->second};
}
} // namespace codegen
} // namespace facebook::velox
