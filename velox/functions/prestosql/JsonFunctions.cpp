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
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/Utf8Utils.h"
#include "velox/functions/prestosql/json/JsonStringUtil.h"
#include "velox/functions/prestosql/json/SIMDJsonUtil.h"
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

/// Class to keep track of json strings being written
/// in to a buffer. The size of the backing buffer must be known during
/// construction time.
class BufferTracker {
 public:
  explicit BufferTracker(BufferPtr buffer) : curPos_(0), currentViewStart_(0) {
    bufPtr_ = buffer->asMutable<char>();
    capacity = buffer->capacity();
  }

  /// Write out all the views to the buffer.
  auto getCanonicalString(JsonViews& jsonViews) {
    for (auto view : jsonViews) {
      trimEscapeWriteToBuffer(view);
    }
    return getStringView();
  }

  /// Sets current view to the end of the previous string.
  /// Should be called only after getCanonicalString ,
  /// as after this call the previous view is lost.
  void startNewString() {
    currentViewStart_ += curPos_;
    curPos_ = 0;
  }

 private:
  /// Trims whitespace and escapes utf characters before writing to buffer.
  void trimEscapeWriteToBuffer(std::string_view input) {
    auto trimmed = velox::util::trimWhiteSpace(input.data(), input.size());
    auto curBufPtr = getCurrentBufferPtr();
    auto bytesWritten =
        prestoJavaEscapeString(trimmed.data(), trimmed.size(), curBufPtr);
    incrementCounter(bytesWritten);
  }

  /// Returns current string view against the buffer.
  std::string_view getStringView() {
    return std::string_view(bufPtr_ + currentViewStart_, curPos_);
  }

  inline char* getCurrentBufferPtr() {
    return bufPtr_ + currentViewStart_ + curPos_;
  }

  void incrementCounter(size_t increment) {
    VELOX_DCHECK_LE(curPos_ + currentViewStart_ + increment, capacity);
    curPos_ += increment;
  }

  size_t capacity;
  size_t curPos_;
  size_t currentViewStart_;
  char* bufPtr_;
};

} // namespace

namespace {
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

class JsonParseFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // Initialize errors here so that we get the proper exception context.
    folly::call_once(
        initializeErrors_, [this] { simdjsonErrorsToExceptions(errors_); });

    VectorPtr localResult;

    // Input can be constant or flat.
    assert(args.size() > 0);
    const auto& arg = args[0];
    if (arg->isConstantEncoding()) {
      auto value = arg->as<ConstantVector<StringView>>()->valueAt(0);
      auto size = value.size();
      if (FOLLY_UNLIKELY(hasInvalidUTF8(value.data(), value.size()))) {
        size = replaceInvalidUTF8Characters(
            paddedInput_.data(), value.data(), value.size());
        paddedInput_.resize(size + simdjson::SIMDJSON_PADDING);
      } else {
        paddedInput_.resize(size + simdjson::SIMDJSON_PADDING);
        memcpy(paddedInput_.data(), value.data(), size);
      }

      auto escapeSize = escapedStringSize(value.data(), size);
      auto buffer = AlignedBuffer::allocate<char>(escapeSize, context.pool());
      BufferTracker bufferTracker{buffer};

      JsonViews jsonViews;

      if (auto error = parse(size, jsonViews)) {
        context.setErrors(rows, errors_[error]);
        return;
      }

      BufferPtr stringViews =
          AlignedBuffer::allocate<StringView>(1, context.pool(), StringView());
      auto rawStringViews = stringViews->asMutable<StringView>();
      rawStringViews[0] =
          StringView(bufferTracker.getCanonicalString(jsonViews));

      auto constantBase = std::make_shared<FlatVector<StringView>>(
          context.pool(),
          JSON(),
          nullptr,
          1,
          stringViews,
          std::vector<BufferPtr>{buffer});

      localResult = BaseVector::wrapInConstant(rows.end(), 0, constantBase);

    } else {
      auto flatInput = arg->asFlatVector<StringView>();
      BufferPtr stringViews = AlignedBuffer::allocate<StringView>(
          rows.end(), context.pool(), StringView());
      auto rawStringViews = stringViews->asMutable<StringView>();

      VELOX_CHECK_LE(rows.end(), flatInput->size());

      size_t maxSize = 0;
      size_t totalOutputSize = 0;
      rows.applyToSelected([&](auto row) {
        auto value = flatInput->valueAt(row);
        maxSize = std::max(maxSize, value.size());
        totalOutputSize += escapedStringSize(value.data(), value.size());
      });

      paddedInput_.resize(maxSize + simdjson::SIMDJSON_PADDING);
      BufferPtr buffer =
          AlignedBuffer::allocate<char>(totalOutputSize, context.pool());
      BufferTracker bufferTracker{buffer};

      rows.applyToSelected([&](auto row) {
        JsonViews jsonViews;
        auto value = flatInput->valueAt(row);
        auto size = value.size();
        if (FOLLY_UNLIKELY(hasInvalidUTF8(value.data(), size))) {
          size = replaceInvalidUTF8Characters(
              paddedInput_.data(), value.data(), size);
          if (maxSize < size) {
            paddedInput_.resize(size + simdjson::SIMDJSON_PADDING);
          }
        } else {
          memcpy(paddedInput_.data(), value.data(), size);
        }

        if (auto error = parse(size, jsonViews)) {
          context.setVeloxExceptionError(row, errors_[error]);
        } else {
          auto canonicalString = bufferTracker.getCanonicalString(jsonViews);

          rawStringViews[row] = StringView(canonicalString);
          bufferTracker.startNewString();
        }
      });

      localResult = std::make_shared<FlatVector<StringView>>(
          context.pool(),
          JSON(),
          nullptr,
          rows.end(),
          stringViews,
          std::vector<BufferPtr>{buffer});
    }

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
  simdjson::error_code parse(size_t size, JsonViews& jsonViews) const {
    simdjson::padded_string_view paddedInput(
        paddedInput_.data(), size, paddedInput_.size());
    SIMDJSON_ASSIGN_OR_RAISE(auto doc, simdjsonParse(paddedInput));
    SIMDJSON_TRY(validate<simdjson::ondemand::document&>(doc, jsonViews));
    if (!doc.at_end()) {
      return simdjson::TRAILING_CONTENT;
    }
    return simdjson::SUCCESS;
  }

  template <typename T>
  static simdjson::error_code validate(T value, JsonViews& jsonViews) {
    SIMDJSON_ASSIGN_OR_RAISE(auto type, value.type());
    switch (type) {
      case simdjson::ondemand::json_type::array: {
        SIMDJSON_ASSIGN_OR_RAISE(auto array, value.get_array());

        jsonViews.push_back(kArrayStart);
        auto jsonViewsSize = jsonViews.size();
        for (auto elementOrError : array) {
          SIMDJSON_ASSIGN_OR_RAISE(auto element, elementOrError);
          SIMDJSON_TRY(validate(element, jsonViews));
          jsonViews.push_back(kSeparator);
        }

        // If the array is not empty, remove the last separator.
        if (jsonViews.size() > jsonViewsSize) {
          jsonViews.pop_back();
        }

        jsonViews.push_back(kArrayEnd);

        return simdjson::SUCCESS;
      }

      case simdjson::ondemand::json_type::object: {
        SIMDJSON_ASSIGN_OR_RAISE(auto object, value.get_object());

        std::vector<std::pair<std::string_view, JsonViews>> objFields;
        for (auto fieldOrError : object) {
          SIMDJSON_ASSIGN_OR_RAISE(auto field, fieldOrError);
          auto key = field.key_raw_json_token();
          JsonViews elementArray;
          SIMDJSON_TRY(validate(field.value(), elementArray));
          objFields.push_back({key, elementArray});
        }

        std::sort(objFields.begin(), objFields.end(), [](auto& a, auto& b) {
          // Remove the quotes from the keys before we sort them.
          auto af = std::string_view{a.first.data() + 1, a.first.size() - 2};
          auto bf = std::string_view{b.first.data() + 1, b.first.size() - 2};
          return lessThan(a.first, b.first);
        });

        jsonViews.push_back(kObjectStart);

        for (auto i = 0; i < objFields.size(); i++) {
          auto field = objFields[i];
          addOrMergeViews(jsonViews, field.first);
          jsonViews.push_back(kObjectKeySeparator);

          for (auto& element : field.second) {
            addOrMergeViews(jsonViews, element);
          }

          if (i < objFields.size() - 1) {
            jsonViews.push_back(kSeparator);
          }
        }

        jsonViews.push_back(kObjectEnd);
        return simdjson::SUCCESS;
      }

      case simdjson::ondemand::json_type::number: {
        SIMDJSON_ASSIGN_OR_RAISE(auto rawJson, value.raw_json());
        addOrMergeViews(jsonViews, rawJson);

        return value.get_double().error();
      }
      case simdjson::ondemand::json_type::string: {
        SIMDJSON_ASSIGN_OR_RAISE(auto rawJson, value.raw_json());
        addOrMergeViews(jsonViews, rawJson);

        return value.get_string().error();
      }

      case simdjson::ondemand::json_type::boolean: {
        SIMDJSON_ASSIGN_OR_RAISE(auto rawJson, value.raw_json());
        addOrMergeViews(jsonViews, rawJson);

        return value.get_bool().error();
      }

      case simdjson::ondemand::json_type::null: {
        SIMDJSON_ASSIGN_OR_RAISE(auto isNull, value.is_null());
        SIMDJSON_ASSIGN_OR_RAISE(auto rawJson, value.raw_json());
        addOrMergeViews(jsonViews, rawJson);

        return isNull ? simdjson::SUCCESS : simdjson::N_ATOM_ERROR;
      }
    }
    VELOX_UNREACHABLE();
  }

  mutable folly::once_flag initializeErrors_;
  mutable std::exception_ptr errors_[simdjson::NUM_ERROR_CODES];
  // Padding is needed in case string view is inlined.
  mutable std::string paddedInput_;
};

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_json_format,
    JsonFormatFunction::signatures(),
    std::make_unique<JsonFormatFunction>());

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_json_parse,
    JsonParseFunction::signatures(),
    [](const std::string& /*name*/,
       const std::vector<exec::VectorFunctionArg>&,
       const velox::core::QueryConfig&) {
      return std::make_shared<JsonParseFunction>();
    });

} // namespace facebook::velox::functions
