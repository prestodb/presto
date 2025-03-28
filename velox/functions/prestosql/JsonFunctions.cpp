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
#include <glog/logging.h>

#include "velox/common/base/SortingNetwork.h"
#include "velox/expression/VectorFunction.h"
#include "velox/expression/VectorWriters.h"
#include "velox/functions/lib/string/StringImpl.h"
#include "velox/functions/prestosql/json/JsonStringUtil.h"
#include "velox/functions/prestosql/json/SIMDJsonExtractor.h"
#include "velox/functions/prestosql/json/SIMDJsonUtil.h"
#include "velox/functions/prestosql/types/JsonCastOperator.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace facebook::velox::functions {

namespace {

const std::string_view kArrayStart = "[";
const std::string_view kArrayEnd = "]";
const std::string_view kSeparator = ",";
const std::string_view kObjectStart = "{";
const std::string_view kObjectEnd = "}";
const std::string_view kObjectKeySeparator = ":";

using JsonViews = std::vector<std::string_view>;

inline void addOrMergeViews(JsonViews& jsonViews, std::string_view view) {
  if (jsonViews.empty()) {
    jsonViews.push_back(view);
    return;
  }

  auto& lastView = jsonViews.back();

  if (lastView.data() + lastView.size() == view.data()) {
    lastView = std::string_view(lastView.data(), lastView.size() + view.size());
  } else {
    jsonViews.push_back(view);
  }
}

void addOrMergeChar(JsonViews& views, std::string_view view) {
  VELOX_DCHECK_EQ(view.size(), 1);
  if (views.empty()) {
    views.push_back(view);
    return;
  }
  auto& last = views.back();
  // OK to do this because input is padded.
  if (*last.end() == view[0]) {
    last = std::string_view(last.data(), last.size() + 1);
  } else {
    views.push_back(view);
  }
}

std::string_view trimToken(std::string_view token) {
  VELOX_DCHECK(!stringImpl::isAsciiWhiteSpace(token[0]));
  auto size = token.size();
  while (stringImpl::isAsciiWhiteSpace(token[size - 1])) {
    --size;
  }
  return std::string_view(token.data(), size);
}

struct JsonField {
  std::string_view key;
  int32_t offset;
  int32_t size;
};

size_t concatViews(const JsonViews& views, char* out) {
  size_t total = 0;
  for (auto& v : views) {
    memcpy(out, v.data(), v.size());
    total += v.size();
    out += v.size();
  }
  return total;
}

class JsonFormatFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VectorPtr localResult;

    // Input can be constant or flat.
    assert(args.size() > 0);
    const auto& arg = args[0];
    if (arg->isConstantEncoding()) {
      auto value = arg->as<ConstantVector<StringView>>()->valueAt(0);
      localResult = std::make_shared<ConstantVector<StringView>>(
          context.pool(), rows.end(), false, VARCHAR(), std::move(value));
    } else {
      auto flatInput = arg->asFlatVector<StringView>();

      auto stringBuffers = flatInput->stringBuffers();
      VELOX_CHECK_LE(rows.end(), flatInput->size());
      localResult = std::make_shared<FlatVector<StringView>>(
          context.pool(),
          VARCHAR(),
          nullptr,
          rows.end(),
          flatInput->values(),
          std::move(stringBuffers));
    }

    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // json -> varchar
    return {exec::FunctionSignatureBuilder()
                .returnType("varchar")
                .argumentType("json")
                .build()};
  }
};

// A performant json parsing implementation. Does not handle null rows. This is
// also leveraged by json functions other than json_parse that need to parse a
// varchar input. If `nullOnError` is true, the result will have null values for
// invalid jsons otherwise it will set exceptions for those rows in 'context'.
class JsonParseImpl {
 public:
  void apply(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& localResult,
      bool nullOnError) const {
    // Initialize errors here so that we get the proper exception context.
    folly::call_once(
        initializeErrors_, [this] { simdjsonErrorsToExceptions(errors_); });

    if (arg->isConstantEncoding()) {
      auto value = arg->as<ConstantVector<StringView>>()->valueAt(0);
      auto size = value.size();
      auto buffer = AlignedBuffer::allocate<char>(size, context.pool());
      BufferPtr stringViews =
          AlignedBuffer::allocate<StringView>(1, context.pool());
      auto rawStringViews = stringViews->asMutable<StringView>();
      try {
        bool needNormalize =
            needNormalizeForJsonParse(value.data(), value.size());

        if (needNormalize) {
          size = normalizedSizeForJsonParse(value.data(), value.size());
        }
        paddedInput_.resize(size + simdjson::SIMDJSON_PADDING);
        VELOX_CHECK_EQ(prepareInput(value, needNormalize), size);

        if (auto error = parse(size, needNormalize)) {
          clearState();
          if (nullOnError) {
            localResult = BaseVector::createNullConstant(
                JSON(), rows.end(), context.pool());
          } else {
            context.setErrors(rows, errors_[error]);
          }
          return;
        }
        auto* output = buffer->asMutable<char>();
        auto outputSize = concatViews(views_, output);
        rawStringViews[0] = StringView(output, outputSize);
      } catch (const VeloxException& e) {
        clearState();

        if (!e.isUserError()) {
          throw;
        }
        if (nullOnError) {
          localResult = BaseVector::createNullConstant(
              JSON(), rows.end(), context.pool());
        } else {
          context.setErrors(rows, std::current_exception());
        }
        FB_LOG_EVERY_MS(WARNING, 1000)
            << "Caught user error in json_parse: " << e.message();
        return;
      }
      auto constantBase = std::make_shared<FlatVector<StringView>>(
          context.pool(),
          JSON(),
          nullptr,
          1,
          stringViews,
          std::vector<BufferPtr>{buffer});

      localResult = BaseVector::wrapInConstant(rows.end(), 0, constantBase);
    }
    VectorPtr jsonInput = arg;
    if (!arg->isFlatEncoding()) {
      BaseVector::flattenVector(jsonInput);
    }
    auto flatInput = jsonInput->asFlatVector<StringView>();
    BufferPtr stringViews = AlignedBuffer::allocate<StringView>(
        rows.end(), context.pool(), StringView());
    auto rawStringViews = stringViews->asMutable<StringView>();

    VELOX_CHECK_LE(rows.end(), flatInput->size());

    size_t maxSize = 0;
    size_t totalOutputSize = 0;
    std::vector<bool> needNormalizes(rows.end());
    auto nullsOnErrors = AlignedBuffer::allocate<bool>(
        rows.end(), context.pool(), bits::kNotNull);
    auto rawNullsOnErrors = nullsOnErrors->asMutable<uint64_t>();
    rows.applyToSelected([&](auto row) {
      auto value = flatInput->valueAt(row);
      bool needNormalize =
          needNormalizeForJsonParse(value.data(), value.size());
      auto size = value.size();
      if (needNormalize) {
        try {
          size = normalizedSizeForJsonParse(value.data(), value.size());
        } catch (const VeloxException& e) {
          if (!e.isUserError()) {
            throw;
          }
          if (!nullOnError) {
            context.setVeloxExceptionError(row, std::current_exception());
          }
          // We use this to skip error-ed out rows when generating output.
          bits::setNull(rawNullsOnErrors, row, true);
          return;
        }
      }
      needNormalizes[row] = needNormalize;
      maxSize = std::max(maxSize, size);
      totalOutputSize += size;
    });

    paddedInput_.resize(maxSize + simdjson::SIMDJSON_PADDING);
    BufferPtr buffer =
        AlignedBuffer::allocate<char>(totalOutputSize, context.pool());
    auto* output = buffer->asMutable<char>();

    rows.applyToSelected([&](auto row) {
      if (bits::isBitNull(rawNullsOnErrors, row)) {
        // Skip if error-ed out earlier.
        return;
      }

      try {
        auto value = flatInput->valueAt(row);
        auto size = prepareInput(value, needNormalizes[row]);
        if (auto error = parse(size, needNormalizes[row])) {
          if (!nullOnError) {
            context.setVeloxExceptionError(row, errors_[error]);
          } else {
            bits::setNull(rawNullsOnErrors, row, true);
          }
          clearState();
          return;
        }
        auto outputSize = concatViews(views_, output);
        rawStringViews[row] = StringView(output, outputSize);
        if (!StringView::isInline(outputSize)) {
          output += outputSize;
        }
      } catch (const VeloxException& e) {
        clearState();

        if (!e.isUserError()) {
          throw;
        }
        if (!nullOnError) {
          context.setVeloxExceptionError(row, std::current_exception());
        } else {
          bits::setNull(rawNullsOnErrors, row, true);
        }
        FB_LOG_EVERY_MS(WARNING, 1000)
            << "Caught user error in json_parse: " << e.message();
      }
    });

    localResult = std::make_shared<FlatVector<StringView>>(
        context.pool(),
        JSON(),
        nullOnError ? nullsOnErrors : nullptr,
        rows.end(),
        stringViews,
        std::vector<BufferPtr>{buffer});
  }

 private:
  struct FastSortKey {
    static constexpr int kSize = 3;
    std::array<uint64_t, kSize> value;
  };

  size_t prepareInput(const StringView& value, bool needNormalize) const {
    size_t outSize;
    if (needNormalize) {
      outSize = normalizeForJsonParse(
          value.data(), value.size(), paddedInput_.data());
    } else {
      memcpy(paddedInput_.data(), value.data(), value.size());
      outSize = value.size();
    }
    memset(paddedInput_.data() + outSize, 0, simdjson::SIMDJSON_PADDING);
    return outSize;
  }

  simdjson::error_code parse(size_t size, bool needNormalize) const {
    simdjson::padded_string_view paddedInput(
        paddedInput_.data(), size, paddedInput_.size());
    SIMDJSON_ASSIGN_OR_RAISE(auto doc, simdjsonParse(paddedInput));
    views_.clear();
    if (needNormalize) {
      SIMDJSON_TRY((generateViews<true, simdjson::ondemand::document&>(doc)));
    } else {
      SIMDJSON_TRY((generateViews<false, simdjson::ondemand::document&>(doc)));
    }
    VELOX_CHECK(fields_.empty());
    if (!doc.at_end()) {
      return simdjson::TRAILING_CONTENT;
    }
    return simdjson::SUCCESS;
  }

  template <bool kNeedNormalize, typename T>
  simdjson::error_code generateViews(T value) const {
    SIMDJSON_ASSIGN_OR_RAISE(auto type, value.type());
    switch (type) {
      case simdjson::ondemand::json_type::array: {
        SIMDJSON_ASSIGN_OR_RAISE(auto array, value.get_array());
        return generateViewsFromArray<kNeedNormalize>(array);
      }
      case simdjson::ondemand::json_type::object: {
        SIMDJSON_ASSIGN_OR_RAISE(auto object, value.get_object());
        return generateViewsFromObject<kNeedNormalize>(object);
      }
      case simdjson::ondemand::json_type::number:
        addOrMergeViews(views_, trimToken(value.raw_json_token()));
        return value.get_double().error();
      case simdjson::ondemand::json_type::string:
        addOrMergeViews(views_, trimToken(value.raw_json_token()));
        // We ask simdjson to allow replacements for invalid UTF-8 sequences.
        // to avoid throwing an exception in line with Presto java.
        return value.get_string(true).error();
      case simdjson::ondemand::json_type::boolean:
        addOrMergeViews(views_, trimToken(value.raw_json_token()));
        return value.get_bool().error();
      case simdjson::ondemand::json_type::null:
        SIMDJSON_ASSIGN_OR_RAISE(auto isNull, value.is_null());
        addOrMergeViews(views_, trimToken(value.raw_json_token()));
        return isNull ? simdjson::SUCCESS : simdjson::N_ATOM_ERROR;
    }
    VELOX_UNREACHABLE();
  }

  template <bool kNeedNormalize>
  simdjson::error_code generateViewsFromArray(
      simdjson::ondemand::array array) const {
    addOrMergeChar(views_, kArrayStart);
    bool first = true;
    for (auto elementOrError : array) {
      SIMDJSON_ASSIGN_OR_RAISE(auto element, elementOrError);
      if (first) {
        first = false;
      } else {
        addOrMergeChar(views_, kSeparator);
      }
      SIMDJSON_TRY(generateViews<kNeedNormalize>(element));
    }
    addOrMergeChar(views_, kArrayEnd);
    return simdjson::SUCCESS;
  }

  template <bool kNeedNormalize>
  simdjson::error_code generateViewsFromObject(
      simdjson::ondemand::object object) const {
    addOrMergeChar(views_, kObjectStart);
    const auto oldNumFields = fields_.size();
    const auto oldNumViews = views_.size();
    for (auto fieldOrError : object) {
      auto offset = views_.size();
      SIMDJSON_ASSIGN_OR_RAISE(auto field, fieldOrError);
      auto key = field.escaped_key();
      views_.push_back({key.data() - 1, key.size() + 2});
      addOrMergeChar(views_, kObjectKeySeparator);
      SIMDJSON_TRY(generateViews<kNeedNormalize>(field.value()));
      auto& newField = fields_.emplace_back();
      newField.key = key;
      newField.offset = offset;
      newField.size = views_.size() - offset;
    }
    sortFields<kNeedNormalize>(
        fields_.data() + oldNumFields,
        fields_.size() - oldNumFields,
        oldNumViews);
    fields_.resize(oldNumFields);
    addOrMergeChar(views_, kObjectEnd);
    return simdjson::SUCCESS;
  }

  template <bool kNeedNormalize>
  void sortFields(const JsonField* fields, int numFields, int oldNumViews)
      const {
    if (numFields <= 1) {
      return;
    }
    const auto sortedBegin = views_.size();
    sortIndices_.resize(numFields);
    std::iota(sortIndices_.begin(), sortIndices_.end(), 0);
    if constexpr (kNeedNormalize) {
      sortIndices([&](int32_t i, int32_t j) {
        return lessThanForJsonParse(fields[i].key, fields[j].key);
      });
    } else if (!fastSort(fields, numFields)) {
      sortIndices(
          [&](int32_t i, int32_t j) { return fields[i].key < fields[j].key; });
    }
    for (auto i = 0; i < numFields; ++i) {
      if (i > 0) {
        addOrMergeChar(views_, kSeparator);
      }
      auto& field = fields[sortIndices_[i]];
      for (int j = 0; j < field.size; ++j) {
        views_.push_back(views_[field.offset + j]);
      }
    }
    auto numNewViews = views_.size() - sortedBegin;
    static_assert(std::is_trivially_copyable_v<std::string_view>);
    memmove(
        &views_[oldNumViews],
        &views_[sortedBegin],
        sizeof(std::string_view) * numNewViews);
    views_.resize(oldNumViews + numNewViews);
  }

  bool fastSort(const JsonField* fields, int numFields) const {
    for (int i = 0; i < numFields; ++i) {
      if (fields[i].key.size() > 8 * FastSortKey::kSize) {
        return false;
      }
    }
    fastSortKeys_.resize(numFields);
    constexpr auto load = [](const char* s) {
      return folly::Endian::big(folly::loadUnaligned<uint64_t>(s));
    };
    for (int i = 0; i < numFields; ++i) {
      const auto& s = fields[i].key;
      auto& t = fastSortKeys_[i].value;
      int j = 0;
      while (8 * (j + 1) <= s.size()) {
        t[j] = load(s.data() + 8 * j);
        ++j;
      }
      auto r = s.size() - 8 * j;
      if (r > 0) {
        auto v = load(s.data() + 8 * j);
        v >>= 8 - r;
        v <<= 8 - r;
        t[j] = v;
      }
    }
    sortIndices([&](int32_t i, int32_t j) {
      return fastSortKeys_[i].value < fastSortKeys_[j].value;
    });
    return true;
  }

  template <typename LessThan>
  void sortIndices(LessThan&& lt) const {
    if (sortIndices_.size() <= kSortingNetworkMaxSize) {
      sortingNetwork(
          sortIndices_.data(), sortIndices_.size(), std::forward<LessThan>(lt));
    } else {
      std::sort(
          sortIndices_.begin(), sortIndices_.end(), std::forward<LessThan>(lt));
    }
  }

  void clearState() const {
    fields_.clear();
    sortIndices_.clear();
    fastSortKeys_.clear();
  }

  mutable folly::once_flag initializeErrors_;
  mutable std::exception_ptr errors_[simdjson::NUM_ERROR_CODES];
  // Padding is needed in case string view is inlined.
  mutable std::string paddedInput_;
  mutable JsonViews views_;
  mutable std::vector<JsonField> fields_;
  mutable std::vector<int32_t> sortIndices_;
  mutable std::vector<FastSortKey> fastSortKeys_;
};

class JsonParseFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // Input can be constant or flat.
    assert(args.size() > 0);
    const auto& arg = args[0];
    VectorPtr localResult;
    parser.apply(
        rows, arg, outputType, context, localResult, false /* nullOnError */);
    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // varchar -> json
    return {exec::FunctionSignatureBuilder()
                .returnType("json")
                .argumentType("varchar")
                .build()};
  }

 private:
  JsonParseImpl parser;
};

template <typename JsonFunction, typename SelectorType>
class JsonFunctionTemplate : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 2);
    VectorPtr localResult;
    applyImpl(rows, args[0], args[1], outputType, context, localResult);

    if (args[0]->type() != JSON()) {
      VELOX_CHECK_EQ(args[0]->type(), VARCHAR());
      // Remove null and error rows as the parser does not expect nulls.
      exec::MutableRemainingRows remainingRows(rows, context);
      if (localResult->rawNulls()) {
        VELOX_CHECK_LE(rows.end(), localResult->size());
        remainingRows.deselectNulls(localResult->rawNulls());
      }
      remainingRows.deselectErrors();
      VectorPtr parsedResult;
      parser_.apply(
          remainingRows.rows(),
          localResult,
          JSON(),
          context,
          parsedResult,
          true /* nullOnError */);
      if (remainingRows.hasChanged()) {
        // Ensure previously removed nulls and errors are set to null and map to
        // a valid row respectively.
        exec::EvalCtx::addNulls(
            rows,
            localResult->rawNulls(),
            context,
            localResult->type(),
            parsedResult);
      }
      localResult = std::move(parsedResult);
    }

    context.moveOrCopyResult(localResult, rows, result);
  }

 private:
  void applyImpl(
      const SelectivityVector& rows,
      const VectorPtr& json,
      const VectorPtr& selector,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& localResult) const {
    if (json->isConstantEncoding() && selector->isConstantEncoding()) {
      bool nullResult = false;
      std::string output;
      if (json->as<ConstantVector<StringView>>()->isNullAt(0) ||
          selector->as<ConstantVector<SelectorType>>()->isNullAt(0)) {
        nullResult = true;
      } else {
        auto jsonValue = json->as<ConstantVector<StringView>>()->valueAt(0);
        auto pathValue =
            selector->as<ConstantVector<SelectorType>>()->valueAt(0);
        try {
          nullResult = JsonFunction::process(jsonValue, pathValue, output) !=
              simdjson::SUCCESS;
        } catch (const VeloxException& e) {
          if (!e.isUserError()) {
            throw;
          }
          nullResult = true;
          context.setErrors(rows, std::current_exception());
        }
      }

      if (nullResult) {
        localResult = BaseVector::createNullConstant(
            outputType, rows.end(), context.pool());
      } else {
        localResult = BaseVector::createConstant(
            outputType, output, rows.end(), context.pool());
      }
      return;
    }
    localResult = context.getVector(outputType, rows.end());
    auto flatResult = localResult->asFlatVector<StringView>();
    VELOX_CHECK_NOT_NULL(flatResult);
    exec::LocalDecodedVector decodedJson(context, *json, rows);
    exec::LocalDecodedVector decodedPath(context, *selector, rows);
    exec::VectorWriter<Json> resultWriter;
    resultWriter.init(*flatResult);

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      VELOX_DCHECK(!decodedPath->isNullAt(row));
      resultWriter.setOffset(row);
      std::string output;
      if (!decodedJson->isNullAt(row) &&
          JsonFunction::process(
              decodedJson->valueAt<StringView>(row),
              decodedPath->valueAt<SelectorType>(row),
              output) == simdjson::SUCCESS) {
        resultWriter.current() = output;
        resultWriter.commit(true);
      } else {
        resultWriter.commit(false);
      }
    });
    resultWriter.finish();
  }

  JsonParseImpl parser_;
};

struct JsonArrayGetImpl {
  static simdjson::error_code
  process(const StringView& jsonArray, int64_t index, std::string& output) {
    simdjson::ondemand::document jsonDoc;

    simdjson::padded_string paddedJson(jsonArray.data(), jsonArray.size());
    if (auto errorCode = simdjsonParse(paddedJson).get(jsonDoc);
        errorCode != simdjson::SUCCESS) {
      return errorCode;
    }
    if (jsonDoc.type().error()) {
      return simdjson::TAPE_ERROR;
    }

    if (jsonDoc.type() != simdjson::ondemand::json_type::array) {
      return simdjson::TAPE_ERROR;
    }

    size_t numElements;
    if (auto errorCode = jsonDoc.count_elements().get(numElements);
        errorCode != simdjson::SUCCESS) {
      return errorCode;
    }

    if (index >= 0) {
      if (index >= numElements) {
        return simdjson::INDEX_OUT_OF_BOUNDS;
      }
    } else if ((int64_t)numElements + index < 0) {
      return simdjson::INDEX_OUT_OF_BOUNDS;
    } else {
      index += numElements;
    }

    std::string_view resultStr;
    if (auto errorCode =
            simdjson::to_json_string(jsonDoc.at(index)).get(resultStr);
        errorCode != simdjson::SUCCESS) {
      return errorCode;
    }

    output = resultStr;
    return simdjson::SUCCESS;
  }
};

struct JsonExtractImpl {
  static simdjson::error_code process(
      const StringView& json,
      const StringView& jsonPath,
      std::string& output) {
    static constexpr std::string_view kNullString{"null"};
    static constexpr std::string_view emptyArrayString{"[]"};
    std::vector<std::string_view> results;
    auto consumer = [&results](auto& v) {
      // We could just convert v to a string using to_json_string directly, but
      // in that case the JSON wouldn't be parsed (it would just return the
      // contents directly) and we might miss invalid JSON.
      SIMDJSON_ASSIGN_OR_RAISE(auto vtype, v.type());
      switch (vtype) {
        case simdjson::ondemand::json_type::object: {
          SIMDJSON_ASSIGN_OR_RAISE(
              auto jsonStr, simdjson::to_json_string(v.get_object()));
          results.push_back(std::move(jsonStr));
          break;
        }
        case simdjson::ondemand::json_type::array: {
          SIMDJSON_ASSIGN_OR_RAISE(
              auto jsonStr, simdjson::to_json_string(v.get_array()));
          results.push_back(std::move(jsonStr));
          break;
        }
        case simdjson::ondemand::json_type::string:
        case simdjson::ondemand::json_type::number:
        case simdjson::ondemand::json_type::boolean: {
          SIMDJSON_ASSIGN_OR_RAISE(auto jsonStr, simdjson::to_json_string(v));
          results.push_back(std::move(jsonStr));
          break;
        }
        case simdjson::ondemand::json_type::null:
          results.push_back(kNullString);
          break;
      }
      return simdjson::SUCCESS;
    };

    auto& extractor = SIMDJsonExtractor::getInstance(jsonPath);
    bool isDefinitePath = true;
    simdjson::padded_string paddedJson(json.data(), json.size());

    // Check for valid json
    {
      SIMDJSON_ASSIGN_OR_RAISE(auto jsonDoc, simdjsonParse(paddedJson));
      simdjson::ondemand::document parsedDoc;
      if (auto errorCode = simdjsonParse(paddedJson).get(parsedDoc);
          errorCode != simdjson::SUCCESS) {
        return errorCode;
      }
    }

    SIMDJSON_TRY(extractor.extract(paddedJson, consumer, isDefinitePath));

    if (results.size() == 0) {
      if (isDefinitePath) {
        // If the path didn't map to anything in the JSON object, return null.
        return simdjson::NO_SUCH_FIELD;
      }
      output = emptyArrayString;
      return simdjson::SUCCESS;
    }
    std::stringstream ss;
    if (!isDefinitePath) {
      ss << "[";
    }
    for (int i = 0; i < results.size(); i++) {
      if (i > 0) {
        ss << ",";
      }
      ss << results[i];
    }
    if (!isDefinitePath) {
      ss << "]";
    }
    output = ss.str();
    return simdjson::SUCCESS;
  }
};

class JsonArrayGetFunction
    : public JsonFunctionTemplate<JsonArrayGetImpl, int64_t> {
 public:
  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        exec::FunctionSignatureBuilder()
            .returnType("json")
            .argumentType("json")
            .argumentType("bigint")
            .build(),
        exec::FunctionSignatureBuilder()
            .returnType("json")
            .argumentType("varchar")
            .argumentType("bigint")
            .build()};
  }
};
class JsonExtractFunction
    : public JsonFunctionTemplate<JsonExtractImpl, StringView> {
 public:
  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        exec::FunctionSignatureBuilder()
            .returnType("json")
            .argumentType("json")
            .argumentType("varchar")
            .build(),
        exec::FunctionSignatureBuilder()
            .returnType("json")
            .argumentType("varchar")
            .argumentType("varchar")
            .build()};
  }
};

// This function is called when $internal$json_string_to_array/map/row
// is called. It is used for expressions like 'Cast(json_parse(x) as
// ARRAY<...>)' etc. This is an optimization to avoid parsing the json string
// twice.
class JsonInternalCastFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 1);
    const auto& arg = *args[0];
    jsonCastOperator_.castFrom(arg, context, rows, resultType, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>>
  signaturesArray() {
    return {exec::FunctionSignatureBuilder()
                .argumentType("varchar")
                .returnType("array(unknown)")
                .build()};
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signaturesMap() {
    return {exec::FunctionSignatureBuilder()
                .argumentType("varchar")
                .returnType("map(unknown, unknown)")
                .build()};
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signaturesRow() {
    return {exec::FunctionSignatureBuilder()
                .argumentType("varchar")
                .returnType("row(unknown)")
                .build()};
  }

 private:
  mutable JsonCastOperator jsonCastOperator_;
};

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_json_format,
    JsonFormatFunction::signatures(),
    std::make_unique<JsonFormatFunction>());

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_json_extract,
    JsonExtractFunction::signatures(),
    [](const std::string& /*name*/,
       const std::vector<exec::VectorFunctionArg>&,
       const velox::core::QueryConfig&) {
      return std::make_shared<JsonExtractFunction>();
    });

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_json_array_get,
    JsonArrayGetFunction::signatures(),
    [](const std::string& /*name*/,
       const std::vector<exec::VectorFunctionArg>&,
       const velox::core::QueryConfig&) {
      return std::make_shared<JsonArrayGetFunction>();
    });

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_json_parse,
    JsonParseFunction::signatures(),
    [](const std::string& /*name*/,
       const std::vector<exec::VectorFunctionArg>&,
       const velox::core::QueryConfig&) {
      return std::make_shared<JsonParseFunction>();
    });

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_$internal$_json_string_to_array,
    JsonInternalCastFunction::signaturesArray(),
    std::make_unique<JsonInternalCastFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_$internal$_json_string_to_map,
    JsonInternalCastFunction::signaturesMap(),
    std::make_unique<JsonInternalCastFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_$internal$_json_string_to_row,
    JsonInternalCastFunction::signaturesRow(),
    std::make_unique<JsonInternalCastFunction>());

} // namespace facebook::velox::functions
