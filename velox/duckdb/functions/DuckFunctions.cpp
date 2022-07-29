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

#include "velox/duckdb/functions/DuckFunctions.h"
#include <boost/algorithm/string.hpp>
#include "velox/duckdb/conversion/DuckConversion.h"
#include "velox/duckdb/memory/Allocator.h"
#include "velox/external/duckdb/duckdb.hpp"
#include "velox/functions/Registerer.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::duckdb {
using ::duckdb::BoundConstantExpression;
using ::duckdb::BoundFunctionExpression;
using ::duckdb::Catalog;
using ::duckdb::CatalogEntry;
using ::duckdb::CatalogType;
using ::duckdb::Connection;
using ::duckdb::DataChunk;
using ::duckdb::Date;
using ::duckdb::DuckDB;
using ::duckdb::ExpressionExecutorState;
using ::duckdb::ExpressionState;
using ::duckdb::LogicalType;
using ::duckdb::LogicalTypeId;
using ::duckdb::ScalarFunction;
using ::duckdb::ScalarFunctionCatalogEntry;
using ::duckdb::Value;
using ::duckdb::Vector;
using ::duckdb::VectorOperations;
using ::duckdb::VectorType;

using ::duckdb::data_ptr_t;
using ::duckdb::date_t;
using ::duckdb::dtime_t;
using ::duckdb::string_t;
using ::duckdb::timestamp_t;
using ::duckdb::ValidityMask;

using exec::EvalCtx;
using exec::Expr;

#define DUCKDB_DYNAMIC_SCALAR_TYPE_DISPATCH(TEMPLATE_FUNC, type, ...)          \
  switch (type.id()) {                                                         \
    case LogicalTypeId::BOOLEAN:                                               \
      TEMPLATE_FUNC<bool>(__VA_ARGS__);                                        \
      break;                                                                   \
    case LogicalTypeId::TINYINT:                                               \
      TEMPLATE_FUNC<int8_t>(__VA_ARGS__);                                      \
      break;                                                                   \
    case LogicalTypeId::SMALLINT:                                              \
      TEMPLATE_FUNC<int16_t>(__VA_ARGS__);                                     \
      break;                                                                   \
    case LogicalTypeId::INTEGER:                                               \
      TEMPLATE_FUNC<int32_t>(__VA_ARGS__);                                     \
      break;                                                                   \
    case LogicalTypeId::BIGINT:                                                \
      TEMPLATE_FUNC<int64_t>(__VA_ARGS__);                                     \
      break;                                                                   \
    case LogicalTypeId::FLOAT:                                                 \
      TEMPLATE_FUNC<float>(__VA_ARGS__);                                       \
      break;                                                                   \
    case LogicalTypeId::DOUBLE:                                                \
      TEMPLATE_FUNC<double>(__VA_ARGS__);                                      \
      break;                                                                   \
    case LogicalTypeId::VARCHAR:                                               \
      TEMPLATE_FUNC<StringView>(__VA_ARGS__);                                  \
      break;                                                                   \
    case LogicalTypeId::TIMESTAMP:                                             \
      TEMPLATE_FUNC<Timestamp>(__VA_ARGS__);                                   \
      break;                                                                   \
    default:                                                                   \
      throw std::runtime_error("Unsupported DuckDB type: " + type.ToString()); \
  }

template <class T>
static void veloxFlatVectorToDuckTemplated(
    VectorPtr input,
    size_t offset,
    size_t /* unused */,
    Vector& result) {
  auto values = input->as<FlatVector<T>>()->values();
  auto valuePtr = (data_ptr_t)values->template as<char>();
  ::duckdb::FlatVector::SetData(result, valuePtr + sizeof(T) * offset);
}

template <>
void veloxFlatVectorToDuckTemplated<StringView>(
    VectorPtr arg,
    size_t offset,
    size_t count,
    Vector& result) {
  // string type needs to be converted
  // it is almost the same, but because DuckDB is null-terminated the length of
  // inlined strings is off-by-one we can almost do zero copy but this breaks on
  // strings of length exactly 12, because velox considers these inlined, but
  // DuckDB considers them not inlined
  // this can be fixed in the future (see DuckDB issue #1036)
  auto resultVarchar = ::duckdb::FlatVector::GetData<string_t>(result);
  auto veloxVarchar = (StringView*)arg->as<FlatVector<StringView>>()
                          ->values()
                          ->as<StringView>();
  auto& duckdbValidity = ::duckdb::FlatVector::Validity(result);
  for (size_t i = 0; i < count; i++) {
    if (duckdbValidity.RowIsValid(i)) {
      resultVarchar[i] =
          DuckStringConversion::toDuck(veloxVarchar[offset + i], result);
    }
  }
}

template <>
void veloxFlatVectorToDuckTemplated<Timestamp>(
    VectorPtr arg,
    size_t offset,
    size_t count,
    Vector& result) {
  // timestamp needs to be converted
  auto resultTimestamp = ::duckdb::FlatVector::GetData<timestamp_t>(result);
  auto veloxTimestamp =
      (Timestamp*)arg->as<FlatVector<Timestamp>>()->values()->as<Timestamp>();
  auto& duckdbValidity = ::duckdb::FlatVector::Validity(result);
  for (size_t i = 0; i < count; i++) {
    if (duckdbValidity.RowIsValid(i)) {
      resultTimestamp[i] =
          DuckTimestampConversion::toDuck(veloxTimestamp[offset + i], result);
    }
  }
}

static void veloxConvertValidity(
    const SelectivityVector& rows,
    VectorPtr arg,
    size_t offset,
    size_t count,
    ValidityMask& result) {
  if (!arg->mayHaveNulls()) {
    // no nulls
    return;
  }

  assert(rows.size() >= (count + offset));

  for (size_t i = 0; i < count; i++) {
    if (rows.isValid(offset + i)) {
      result.SetValid(i);
    } else {
      result.SetInvalid(i);
    }
  }
}

static void veloxConvertSelectionVector(
    const SelectivityVector& rows,
    size_t offset,
    size_t count,
    ValidityMask& result) {
  if (rows.isAllSelected()) {
    // nothing selected
    return;
  }
  for (size_t i = 0; i < count; i++) {
    if (!rows.isValid(offset + i)) {
      result.SetInvalid(i);
    }
  }
}

static void veloxFlatVectorToDuck(
    const SelectivityVector& /*rows*/,
    VectorPtr arg,
    size_t offset,
    size_t count,
    Vector& result) {
  DUCKDB_DYNAMIC_SCALAR_TYPE_DISPATCH(
      veloxFlatVectorToDuckTemplated,
      result.GetType(),
      arg,
      offset,
      count,
      result);
}

template <class T>
static void veloxConstantVectorToDuckTemplated(VectorPtr arg, Vector& result) {
  auto constant = arg->as<ConstantVector<T>>();
  Vector v(result.GetType());
  v.SetVectorType(VectorType::CONSTANT_VECTOR);
  v.SetValue(0, constant->valueAt(0));
  result.Reference(v);
}

template <>
void veloxConstantVectorToDuckTemplated<StringView>(
    VectorPtr arg,
    Vector& result) {
  auto constant = arg->as<ConstantVector<StringView>>()->valueAt(0);
  Vector v(result.GetType());
  v.SetVectorType(VectorType::CONSTANT_VECTOR);
  v.SetValue(0, std::string(constant.data(), constant.size()));
  result.Reference(v);
}

template <>
void veloxConstantVectorToDuckTemplated<Timestamp>(
    VectorPtr arg,
    Vector& result) {
  auto constant = arg->as<ConstantVector<Timestamp>>()->valueAt(0);
  Vector v(result.GetType());
  v.SetVectorType(VectorType::CONSTANT_VECTOR);
  v.SetValue(0, Value::TIMESTAMP(veloxTimestampToDuckDB(constant)));
  result.Reference(v);
}

static void veloxConstantVectorToDuck(VectorPtr arg, Vector& result) {
  if (arg->isNullAt(0)) {
    Value v(result.GetType());
    result.Reference(v);
  } else {
    DUCKDB_DYNAMIC_SCALAR_TYPE_DISPATCH(
        veloxConstantVectorToDuckTemplated, result.GetType(), arg, result);
  }
}

template <class T>
static void veloxDecodedVectorToDuckTemplated(
    DecodedVector& arg,
    size_t offset,
    size_t count,
    Vector& result) {
  auto value_ptr = (data_ptr_t)arg.data<T>();
  ::duckdb::FlatVector::SetData(result, value_ptr);

  // construct a selection vector from the indices of the decoded vector
  auto indices = arg.indices();
  ::duckdb::SelectionVector sel(STANDARD_VECTOR_SIZE);
  for (idx_t i = 0; i < count; i++) {
    sel.set_index(i, indices[offset + i]);
  }

  // slice the vector
  result.Slice(sel, count);
}

template <class VELOXTYPE, class DUCKTYPE, class CONVERSION>
void veloxDecodedVectorConversion(
    DecodedVector& arg,
    size_t offset,
    size_t count,
    Vector& result) {
  auto resultData = ::duckdb::FlatVector::GetData<DUCKTYPE>(result);
  auto& resultValidity = ::duckdb::FlatVector::Validity(result);
  auto veloxData = arg.data<VELOXTYPE>();
  auto veloxIndices = arg.indices();
  auto veloxNulls = arg.nulls();
  if (veloxNulls) {
    for (size_t i = 0; i < count; i++) {
      auto idx = veloxIndices[offset + i];
      if (!bits::isBitNull(veloxNulls, idx)) {
        resultData[i] = CONVERSION::toDuck(veloxData[idx], result);
      } else {
        resultValidity.SetInvalid(i);
      }
    }
  } else {
    for (size_t i = 0; i < count; i++) {
      auto idx = veloxIndices[offset + i];
      resultData[i] = CONVERSION::toDuck(veloxData[idx], result);
    }
  }
}

template <>
void veloxDecodedVectorToDuckTemplated<StringView>(
    DecodedVector& arg,
    size_t offset,
    size_t count,
    Vector& result) {
  // string type needs to be converted, see veloxFlatVectorToDuckTemplated
  veloxDecodedVectorConversion<StringView, string_t, DuckStringConversion>(
      arg, offset, count, result);
}

template <>
void veloxDecodedVectorToDuckTemplated<Timestamp>(
    DecodedVector& arg,
    size_t offset,
    size_t count,
    Vector& result) {
  veloxDecodedVectorConversion<Timestamp, timestamp_t, DuckTimestampConversion>(
      arg, offset, count, result);
}

static void veloxDecodedVectorToDuck(
    DecodedVector& arg,
    size_t offset,
    size_t count,
    Vector& result) {
  DUCKDB_DYNAMIC_SCALAR_TYPE_DISPATCH(
      veloxDecodedVectorToDuckTemplated,
      result.GetType(),
      arg,
      offset,
      count,
      result);
}

static void toDuck(
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args,
    size_t offset,
    DataChunk& castChunk,
    DataChunk& result) {
  if (args.size() == 0) {
    return;
  }
  auto numRows = args[0]->size();
  auto cardinality = std::min<size_t>(numRows - offset, STANDARD_VECTOR_SIZE);
  result.SetCardinality(cardinality);

  ValidityMask selMask(cardinality);
  veloxConvertSelectionVector(rows, offset, cardinality, selMask);

  assert(!castChunk.data.empty());
  assert(!result.data.empty());
  for (idx_t i = 0; i < args.size(); i++) {
    auto& arg = args[i];
    auto& sourceType = castChunk.data[i].GetType();
    auto& inputType = result.data[i].GetType();
    bool requiresCast = sourceType != inputType;
    auto target = requiresCast ? &castChunk : &result;
    if (requiresCast) {
      castChunk.Reset();
    }
    switch (arg->encoding()) {
      case VectorEncoding::Simple::FLAT: {
        auto& nullMask = ::duckdb::FlatVector::Validity(target->data[i]);
        veloxConvertValidity(rows, arg, offset, cardinality, nullMask);

        for (size_t i = 0; i < cardinality; i++) {
          if (!selMask.RowIsValid(i)) {
            nullMask.SetInvalid(i);
          }
        }

        veloxFlatVectorToDuck(rows, arg, offset, cardinality, target->data[i]);
        break;
      }
      case VectorEncoding::Simple::CONSTANT:
        veloxConstantVectorToDuck(arg, target->data[i]);
        break;
      default: {
        DecodedVector decoded;
        decoded.decode(*arg, rows, false);
        veloxDecodedVectorToDuck(decoded, offset, cardinality, target->data[i]);
        break;
      }
    }
    if (requiresCast) {
      VectorOperations::Cast(castChunk.data[i], result.data[i], cardinality);
    }
  }
  result.Verify();
}

VectorPtr createVeloxVector(
    const SelectivityVector& rows,
    LogicalType type,
    size_t count,
    exec::EvalCtx* context) {
  auto resultType = toVeloxType(type);
  auto result = BaseVector::create(resultType, count, context->pool());
  context->ensureWritable(rows, resultType, result);
  return result;
}

namespace {

struct DuckDBFunctionData {
  DuckDBFunctionData()
      : expr(Value::INTEGER(42)), state(expr, executor_state) {}

  //! The currently bound function
  size_t functionIndex;
  // input data chunk
  std::unique_ptr<DataChunk> input;
  std::unique_ptr<DataChunk> castChunk;
  //! The input types
  std::vector<LogicalType> inputTypes;
  // dummy stuff, necessary to call the function but not actually used
  BoundConstantExpression expr;
  ExpressionExecutorState executor_state{"N/A"};
  ExpressionState state;
};

class DuckDBFunction : public exec::VectorFunction {
 public:
  explicit DuckDBFunction(std::vector<ScalarFunction> set) : set_(move(set)) {
    assert(set_.size() > 0);
  }

 private:
  mutable std::vector<ScalarFunction> set_;

 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      EvalCtx* context,
      VectorPtr* result) const override {
    // FIXME: this binding does not need to be performed on every function call
    std::vector<LogicalType> inputTypes;
    for (auto& arg : args) {
      inputTypes.push_back(fromVeloxType(arg->typeKind()));
    }

    duckdb::VeloxPoolAllocator duckDBAllocator(*context->pool());
    auto state = initializeState(move(inputTypes), duckDBAllocator);
    assert(state->functionIndex < set_.size());
    auto& function = set_[state->functionIndex];
    idx_t nrow = rows.size();

    if (!*result) {
      *result = createVeloxVector(rows, function.return_type, nrow, context);
    }
    for (auto offset = 0; offset < nrow; offset += STANDARD_VECTOR_SIZE) {
      // reset the input chunk by referencing the base chunk
      state->input->Reset();
      // convert arguments to duck arguments
      toDuck(rows, args, offset, *state->castChunk, *state->input);
      // run the function
      callFunction(function, *state, offset, *result);
    }
  }

  template <class T>
  void callFunctionNumeric(
      ScalarFunction& function,
      DuckDBFunctionData& state,
      size_t offset,
      VectorPtr result) const {
    // all other supported types (numerics) have the same representation between
    // velox and duckdb we can thus zero-copy that type we create a duckdb
    // Vector that uses as base data the velox data then run the function, which
    // fills in the velox vector
    auto dataPointer = (data_ptr_t)result->valuesAsVoid();
    dataPointer += sizeof(T) * offset;
    Vector resultVector(function.return_type, dataPointer);
    function.function(*state.input, state.state, resultVector);
    switch (resultVector.GetVectorType()) {
      case VectorType::FLAT_VECTOR:
        // result was already written to the velox vector
        break;
      case VectorType::CONSTANT_VECTOR: {
        if (::duckdb::ConstantVector::IsNull(resultVector)) {
          for (auto i = 0; i < state.input->size(); i++) {
            auto veloxIndex = offset + i;
            result->setNull(veloxIndex, true);
          }
        } else {
          auto resultEntry = ::duckdb::ConstantVector::GetData<T>(resultVector);
          auto veloxData = (T*)result->valuesAsVoid();
          for (auto i = 0; i < state.input->size(); i++) {
            auto veloxIndex = offset + i;
            veloxData[veloxIndex] = *resultEntry;
          }
        }
        break;
      }
      default:
        throw std::runtime_error("unexpected vector type");
    }
  }

  template <class SRC, class TGT, class OP>
  void callFunctionConversion(
      ScalarFunction& function,
      DuckDBFunctionData& state,
      size_t offset,
      VectorPtr result) const {
    Vector resultVector(function.return_type);
    function.function(*state.input, state.state, resultVector);
    auto flatResult = result->asFlatVector<TGT>();
    switch (resultVector.GetVectorType()) {
      case VectorType::FLAT_VECTOR: {
        auto resultData = ::duckdb::FlatVector::GetData<SRC>(resultVector);
        auto& resultMask = ::duckdb::FlatVector::Validity(resultVector);
        for (auto i = 0; i < state.input->size(); i++) {
          auto veloxIndex = offset + i;
          if (resultMask.RowIsValid(i)) {
            flatResult->set(veloxIndex, OP::toVelox(resultData[i]));
          }
        }
        break;
      }
      case VectorType::CONSTANT_VECTOR: {
        if (::duckdb::ConstantVector::IsNull(resultVector)) {
          for (auto i = 0; i < state.input->size(); i++) {
            auto veloxIndex = offset + i;
            result->setNull(veloxIndex, true);
          }
        } else {
          auto resultData =
              ::duckdb::ConstantVector::GetData<SRC>(resultVector);
          auto convertedEntry = OP::toVelox(*resultData);
          for (auto i = 0; i < state.input->size(); i++) {
            auto veloxIndex = offset + i;
            flatResult->set(veloxIndex, convertedEntry);
          }
        }
        break;
      }
      default:
        throw std::runtime_error("unexpected vector type");
    }
  }

  void callFunction(
      ScalarFunction& function,
      DuckDBFunctionData& state,
      size_t offset,
      VectorPtr result) const {
    switch (function.return_type.id()) {
      case LogicalTypeId::BOOLEAN:
        callFunctionNumeric<bool>(function, state, offset, result);
        break;
      case LogicalTypeId::TINYINT:
        callFunctionNumeric<int8_t>(function, state, offset, result);
        break;
      case LogicalTypeId::SMALLINT:
        callFunctionNumeric<int16_t>(function, state, offset, result);
        break;
      case LogicalTypeId::INTEGER:
        callFunctionNumeric<int32_t>(function, state, offset, result);
        break;
      case LogicalTypeId::BIGINT:
        callFunctionNumeric<int64_t>(function, state, offset, result);
        break;
      case LogicalTypeId::FLOAT:
        callFunctionNumeric<float>(function, state, offset, result);
        break;
      case LogicalTypeId::DOUBLE:
        callFunctionNumeric<double>(function, state, offset, result);
        break;
      case LogicalTypeId::TIMESTAMP:
        callFunctionConversion<timestamp_t, Timestamp, DuckTimestampConversion>(
            function, state, offset, result);
        break;
      case LogicalTypeId::VARCHAR:
        callFunctionConversion<string_t, StringView, DuckStringConversion>(
            function, state, offset, result);
        break;
      default:
        break;
    }
  }

  bool isDefaultNullBehavior() const override {
    // this is true for all currently exported duckdb functions, but should
    // probably be fixed later on
    return true;
  }

 private:
  std::unique_ptr<DuckDBFunctionData> initializeState(
      std::vector<LogicalType> inputTypes,
      duckdb::VeloxPoolAllocator& duckDBAllocator) const {
    assert(set_.size() > 0);
    auto result = std::make_unique<DuckDBFunctionData>();
    result->inputTypes = move(inputTypes);
    std::string error;
    bool castParameters;
    result->functionIndex = ::duckdb::Function::BindFunction(
        set_[0].name, set_, result->inputTypes, error, castParameters);
    if (result->functionIndex >= set_.size()) {
      // binding error: cannot bind this function with the provided parameters
      throw std::runtime_error(error);
    }
    // figure out the expected types
    auto& function = set_[result->functionIndex];
    std::vector<LogicalType> expectedTypes = function.arguments;
    // handle varargs
    for (size_t i = function.arguments.size(); i < result->inputTypes.size();
         i++) {
      if (function.varargs.id() == LogicalTypeId::ANY) {
        // function supports any type of vararg: use the input type directly
        expectedTypes.push_back(result->inputTypes[i]);
      } else {
        // function requires a specific vararg type
        expectedTypes.push_back(function.varargs);
      }
    }
    // initialize the base chunks

    result->input = std::make_unique<DataChunk>();
    result->castChunk = std::make_unique<DataChunk>();
    result->input->Initialize(duckDBAllocator, expectedTypes);
    result->castChunk->Initialize(duckDBAllocator, result->inputTypes);
    return result;
  }
};

} // namespace

bool functionIsOperator(const std::string& name) {
  // normal functions have ascii letters and underscores
  // operators have non-ascii characters
  // bit hacky but eh
  for (int i = 0; i < name.size(); i++) {
    char c = name[i];
    if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_')) {
      return true;
    }
  }
  return false;
}

namespace {
std::string toString(const TypePtr& type) {
  std::ostringstream out;
  out << boost::algorithm::to_lower_copy(std::string(type->kindName()));
  if (type->size()) {
    out << "(";
    for (auto i = 0; i < type->size(); i++) {
      if (i > 0) {
        out << ",";
      }
      out << toString(type->childAt(i));
    }
    out << ")";
  }
  return out.str();
}
} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> getSignatures(
    const std::vector<ScalarFunction>& functions) {
  std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
  signatures.reserve(functions.size());
  for (auto const& function : functions) {
    exec::FunctionSignatureBuilder builder;

    builder.returnType(toString(toVeloxType(function.return_type)));
    for (const auto& argType : function.arguments) {
      builder.argumentType(toString(toVeloxType(argType)));
    }
    if (function.varargs.id() != LogicalTypeId::INVALID) {
      builder.argumentType(toString(toVeloxType(function.varargs)));
      builder.variableArity();
    }

    signatures.emplace_back(builder.build());
  }
  return signatures;
}

void registerDuckDBFunction(CatalogEntry* entry, const std::string& prefix) {
  if (entry->type != CatalogType::SCALAR_FUNCTION_ENTRY) {
    return;
  }
  auto& scalarFunction = (ScalarFunctionCatalogEntry&)*entry;
  auto name = scalarFunction.name;
  if (name == "~~") {
    name = "like";
  } else if (name == "!~~") {
    name = "not_like";
  } else if (name == "~~~") {
    name = "glob";
  }
  // skip any operators (e.g. addition, etc)
  if (functionIsOperator(name)) {
    return;
  }
  // these functions are skipped because they are normally resolved in the
  // binding phase
  std::unordered_set<std::string> skippedFunctions{"typeof", "alias"};
  if (skippedFunctions.find(name) != skippedFunctions.end()) {
    return;
  }
  std::vector<ScalarFunction> functions;
  for (size_t i = 0; i < scalarFunction.functions.size(); i++) {
    auto& testedFunction = scalarFunction.functions[i];
    // functions that need to be bound are not supported (yet?)
    if (testedFunction.bind || testedFunction.dependency) {
      continue;
    }
    if (!duckdbTypeIsSupported(testedFunction.return_type)) {
      continue;
    }
    bool allArgsSupported = true;
    for (auto& arg : testedFunction.arguments) {
      if (!duckdbTypeIsSupported(arg)) {
        allArgsSupported = false;
        break;
      }
    }
    if (!allArgsSupported) {
      continue;
    }
    functions.push_back(testedFunction);
  }
  if (functions.size() == 0) {
    return;
  }
  auto signatures = getSignatures(functions);
  exec::registerVectorFunction(
      prefix + name,
      signatures,
      std::make_unique<DuckDBFunction>(move(functions)));
}

void registerDuckdbFunctions(const std::string& prefix) {
  DuckDB db;
  Connection con(db);
  con.context->transaction.BeginTransaction();
  auto schema = Catalog::GetCatalog(*con.context).GetSchema(*con.context);
  schema->Scan(
      *con.context,
      CatalogType::SCALAR_FUNCTION_ENTRY,
      [&](CatalogEntry* entry) { registerDuckDBFunction(entry, prefix); });
}

} // namespace facebook::velox::duckdb
