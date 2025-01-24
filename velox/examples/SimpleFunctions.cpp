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

#include <re2/re2.h>
#include "velox/functions/Udf.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/types/JsonType.h"

using namespace facebook::velox;

namespace {

/// This file contains examples of common patterns used to build simple
/// functions. A higher-level description of the different features provided by
/// the framework and their semantics can be found at:
///  `velox/docs/develop/scalar-functions.rst`

//
// Our first, very simple function.
//

// Simple functions (as opposed to vector functions) are functions that
// operate on one row at a time. To ensure high performance and avoid virtual
// call dispatching per row, the framework is solely based on templating.
//
// Let's start by defining a very simple function that returns the sum of the
// two input parameters. A few important things are:
//
// - The class needs to define a `call()` method which will get called for each
//   input row. FOLLY_ALWAYS_INLINE may be used to ensure the compiler always
//   inlines it.
//
// - The first parameter for `call()` is always the return value of the function
//   (taken as a non-const ref), followed by the remaining function parameters
//   (taken as const refs).
//
// - Input parameters are not nullable by default; the expression eval path
//   assumes that any null values in the inputs will produce null as output, and
//   skips the actual `call()` execution, as an optimization. Check the examples
//   below if you need to change this behavior and take nullable inputs.
//
// - The `call()` method may return bool or void. Bool controls the nullability
//   of the output value (true means not null, false means null); void means the
//   function never returns null.
//
// - The struct/class needs to take a template parameter (TExecParams), which
//   will be used in the next examples to define string and complex types (this
//   is a legacy requirement and will be removed in the future).
template <typename TExecParams>
struct MyPlusFunction {
  FOLLY_ALWAYS_INLINE void
  call(int64_t& out, const int64_t& a, const int64_t& b) {
    out = a + b;
  }
};

// Functions need to be registered before they can be used. Registering a
// function is where the template instantiation happens. The first template
// parameter is the class/struct defined, followed by the function return type,
// and input parameters.
//
// The registration function takes a vector of names (or aliases) by which the
// function will be accessible.
void register1() {
  registerFunction<MyPlusFunction, int64_t, int64_t, int64_t>({"my_plus"});

  // All standard cpp type deduction and implicit casts are still valid when
  // matching the types provided here with the ones available in the `call()`
  // methods. Hence, relying on implicit type conversion is valid in this case
  // (though not encouraged):
  registerFunction<MyPlusFunction, int64_t, int8_t, int8_t>({"my_plus_small"});
}

//
// Templated functions.
//

// In order to make the function code more generic, simple functions can also
// provide templated `call()` methods based on the input parameters:
template <typename TExecParams>
struct MyPlusTemplatedFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& out, const TInput& a, const TInput& b) {
    out = a + b;
  }

  // You can overload for specific types to specialize.
  FOLLY_ALWAYS_INLINE void call(double& out, const double& a, const double& b) {
    out = (std::isnan(a) ? 0 : a) + (std::isnan(b) ? 0 : b);
  }
};

// And again, registration is where template instantiation happens.
void register2() {
  registerFunction<MyPlusTemplatedFunction, double, double, double>(
      {"my_plus_double"});

  registerFunction<MyPlusTemplatedFunction, int16_t, int16_t, int16_t>(
      {"my_plus_smallint"});

  // `velox/functions/lib/RegistrationHelpers.h` provides helper functions to
  // prevent users from repeating tedious type combinations when instantiating
  // templates. In this particular case, one could just:
  functions::registerBinaryNumeric<MyPlusTemplatedFunction>({"my_other_plus"});
}

//
// Changing null behavior.
//

// If having nulls in one of the inputs does not imply null in the output, a
// function can change the null behavior by overwriting the `callNullable()`
// method, instead of `call()`. The main difference being that the expected
// signature takes input parameters as pointers instead of const refs.
//
// Also, both call() and callNullabe() can be implemented returning a boolean
// controling the nullability of the output result (true means not null).
template <typename TExecParams>
struct MyNullablePlusFunction {
  FOLLY_ALWAYS_INLINE bool
  callNullable(int64_t& out, const int64_t* a, const int64_t* b) {
    out = (a == nullptr ? 0 : *a) + (b == nullptr ? 0 : *b);
    return true;
  }
};

void register3() {
  registerFunction<MyNullablePlusFunction, int64_t, int64_t, int64_t>(
      {"my_nullable_plus"});
}

//
// Determinism.
//

// By default, simple functions are assumed to be deterministic, i.e, they must
// produce the same output given the same input parameters. This fact might be
// used by the engine an as optimization in some cases, e.g. running the
// function just once if the input column contains a constant value. If that's
// not the case, a function can declare non-deterministic behavior by setting
// the following flag:
template <typename TExecParams>
struct MyNonDeterministicFunction {
  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE bool call(double& result) {
    result = folly::Random::randDouble01();
    return true;
  }
};

void register4() {
  registerFunction<MyNonDeterministicFunction, double>(
      {"my_non_deterministic_func"});
}

//
// String types.
//

// String and complex types are made available through wrapper types for input
// (arg_type<>) and output (out_type<>) values.
//
// Velox supports two string types: VARBINARY and VARCHAR. Both types will be
// instantiated as `StringView` for input values and `StringWriter` for output,
// but VARBINARY semantically represents an opaque stream of bytes, while
// VARCHAR values need to be charset encoding aware, and properly handle UTF-8
// characters, for instance.
//
// In order to define these types, use the
// VELOX_DEFINE_FUNCTION_TYPES(TExecParams) macro:
template <typename TExecParams>
struct MyStringConcatFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& varcharInput,
      const arg_type<Varbinary>& varbinaryInput) {
    // `result` is instantiated as StringWriter; `varcharInput` and
    // `varbinaryInput` are StringViews.
    result.resize(varcharInput.size() + varbinaryInput.size());
    std::memcpy(result.data(), varcharInput.data(), varcharInput.size());
    std::memcpy(
        result.data() + varcharInput.size(),
        varbinaryInput.data(),
        varbinaryInput.size());
    return true;
  }
};

// Since both VARCHAR and VARBINARY types resolve to the same underlying cpp
// types, it is valid to use them interchangeably while registering functions,
// though not encouraged. We recommend users to template the `call()` function
// to make the intention clearer:
template <typename TExecParams>
struct MyGenericStringConcatFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

  template <typename TResult, typename TInput1, typename TInput2>
  FOLLY_ALWAYS_INLINE bool
  call(TResult& result, const TInput1& input1, const TInput2& input2) {
    result.resize(input1.size() + input2.size());
    std::memcpy(result.data(), input1.data(), input1.size());
    std::memcpy(result.data() + input1.size(), input2.data(), input2.size());
    return true;
  }
};

void register5() {
  registerFunction<MyStringConcatFunction, Varchar, Varchar, Varbinary>(
      {"my_string_concat"});

  // Any combinations of input and output string types can now be instantiated:
  registerFunction<MyGenericStringConcatFunction, Varchar, Varchar, Varbinary>(
      {"my_generic_string_concat"});
  registerFunction<
      MyGenericStringConcatFunction,
      Varbinary,
      Varbinary,
      Varchar>({"my_generic_string_concat"});

  registerFunction<MyGenericStringConcatFunction, Varbinary, Varchar, Varchar>(
      {"my_generic_string_concat"});
}

//
// Advanced string processing.
//

// Functions are supposed to work correctly on UTF-8 data. Optionally, functions
// can also provide a `callAscii()` method, which will be automatically called
// by the engine in case it detects that the input is composed of only ASCII
// characters:
template <typename TExecParams>
struct MyAsciiAwareFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

  // Regular `call()` method, which needs to correctly handle UTF-8 input.
  FOLLY_ALWAYS_INLINE bool call(out_type<Varchar>&, const arg_type<Varchar>&) {
    return true;
  }

  // Fast ascii-only path, which can assume the input has only ASCII characters.
  FOLLY_ALWAYS_INLINE bool callAscii(
      out_type<Varchar>&,
      const arg_type<Varchar>&) {
    return true;
  }

  // Optionally, a function can declare "default ascii behavior", indicating
  // that any produced output strings will be ascii-only, in case all
  // inputs are ascii-only. This hint allows the engine to avoid running
  // character set detection on the output of this function.
  static constexpr bool is_default_ascii_behavior = true;
};

// In some cases, the user might want to reuse the String buffer from one of
// the inputs in the output, making the function zero-copy. Some compelling
// examples are trim (ltrim and rtrim), substr, and split. One can do that by
// setting the flag below, which specifies the index of the argument whose
// strings are being re-used in the output. Valid output types are VARCHAR and
// ARRAY<VARCHAR>.
//
// This example implements a simple split function that tokenizes the input
// string based on empty spaces (' '), returning an array of strings that reuse
// the same buffer as the first parameter (zero-copy). Check the "Complex Types"
// section below for more examples about arrays, maps, rows and other complex
// types.
template <typename TExecParams>
struct MySimpleSplitFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

  // Results refer to the first input strings parameter buffer.
  static constexpr int32_t reuse_strings_from_arg = 0;

  const char splitChar{' '};

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Array<Varchar>>& out,
      const arg_type<Varchar>& input) {
    auto start = input.begin();
    auto cur = start;

    // This code doesn't copy the string contents.
    do {
      cur = std::find(start, input.end(), splitChar);
      out.add_item().copy_from(StringView(start, cur - start));
      start = cur + 1;
    } while (cur < input.end());
    return true;
  }
};

void register6() {
  registerFunction<MyAsciiAwareFunction, Varchar, Varchar>(
      {"my_ascii_aware_func"});

  registerFunction<MySimpleSplitFunction, Array<Varchar>, Varchar>(
      {"my_simple_split_func"});
}

//
// Custom initialization and constant inputs.
//

// It is possible for simple functions to pre-process session/query configs and
// constant inputs, and possibly hold state by providing an `initialize()`
// method. This method has void return type, and takes a QueryConfig in addition
// to pointers to the input parameters declared during function registration.
// The pointers will carry the constant values (for any inputs containing
// constant values), or null in case the inputs are not constant.
//
// The example below illustrates a toy implementation of an RE2-based regular
// expression function, which compiles the regexp pattern only once if the
// pattern is constant (which is the common case):
template <typename TExecParams>
struct MyRegexpMatchFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig&,
      const arg_type<Varchar>*,
      const arg_type<Varchar>* pattern) {
    // Compile the RE2 object just once if pattern is constant. Functions might
    // choose to throw in case the regexp pattern is not constant, as it can be
    // quite expensive to compile it on a per-row basis. In this example we
    // support both modes (const and non-const).
    if (pattern != nullptr) {
      re_.emplace(*pattern);
    }

    // Optionally, one could also inspect the session configs in `QueryConfig`.
    // One common use case is to initialize user supplied session timezone for
    // date/time manipulation functions.
  }

  FOLLY_ALWAYS_INLINE bool call(
      bool& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& pattern) {
    // Check if the pattern is constant and was already initialized, e.g:
    // >  `my_regexp_match(col1, "^.*$")`
    // or, if it is not constant, we need to compile it for each row, e.g:
    // > `my_regexp_match(col1, col2)`
    result = re_.has_value()
        ? RE2::PartialMatch(toStringPiece(input), *re_)
        : RE2::PartialMatch(toStringPiece(input), ::re2::RE2(pattern));
    return true;
  }

  template <typename TString>
  re2::StringPiece toStringPiece(const TString& input) {
    return re2::StringPiece(input.data(), input.size());
  }

  std::optional<::re2::RE2> re_;
};

void register7() {
  registerFunction<MyRegexpMatchFunction, bool, Varchar, Varchar>(
      {"my_regexp_match"});
}

//
// Other complex types - Arrays, Maps, Rows, and Opaque.
//

// Similarly to strings, complex types such as Arrays, Maps, and Rows, can be
// accessed through proxy objects. These proxy objects are currently implemented
// using std containers (see comments below).
//
// The iterators provided by the proxy objects are based on std::optional, to
// represent null values inside the containers themselves. This is true for
// Arrays, Maps, and Rows.
//
// Opaque types are a way to allow users to push user-defined objects to the
// function code by wrapping them in a shared_ptr. Check
// `velox/examples/OpaqueType.cpp` for a more thorough example of Opaque type
// usage. The class below represents the data we will push to the function code
// using Opaque types:
struct UserDefinedObject {
  int data;
};

// Define a toy function that doubles the input parameters provided as an Array,
// Map, Row, or Opaque<UserDefinedObject> type.
template <typename TExecParams>
struct MyComplexTimesTwoFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

  // Just for fun, we'll define overloaded `call()` methods to handle different
  // complex types. This one takes and returns an Array. Arrays proxy objects
  // have their APIs based on std::vector but the data is not backed by
  // std::vector. Vector elements are currently wrapped by std::optional to
  // represent their nullability.
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Array<int64_t>>& result,
      const arg_type<Array<int64_t>>& inputArray) {
    result.reserve(inputArray.size());
    for (const auto& it : inputArray) {
      result.push_back(it.has_value() ? it.value() * 2 : 0);
    }
    return true;
  }

  // This method takes and returns a Map. Map proxy objects have their APIs
  // based on std::unordered_map but the data is not actually backed by it;
  // Values are wrapped by std::optional.
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Map<int64_t, double>>& result,
      const arg_type<Map<int64_t, double>>& inputMap) {
    result.reserve(inputMap.size());
    for (const auto& it : inputMap) {
      result.emplace(
          it.first * 2, it.second.has_value() ? it.second.value() * 2 : 0);
    }
    return true;
  }

  // Takes and returns a Row. Rows are backed by std::tuple; individual elements
  // are std::optional.
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Row<int64_t, double>>& result,
      const arg_type<Row<int64_t, double>>& inputRow) {
    const auto& elem0 = inputRow.template at<0>();
    const auto& elem1 = inputRow.template at<1>();

    // For Rows, let's make the function return NULL in case any of the elements
    // of the Row are null.
    if (!elem0.has_value() || !elem1.has_value()) {
      return false;
    }
    result = std::make_tuple(
        std::make_optional(*elem0 * 2), std::make_optional(*elem1 * 2));
    return true;
  }

  // Method that takes and returns an Opaque type (UserDefinedObject)
  FOLLY_ALWAYS_INLINE bool call(
      arg_type<std::shared_ptr<UserDefinedObject>>& output,
      const arg_type<std::shared_ptr<UserDefinedObject>>& input) {
    output =
        std::make_shared<UserDefinedObject>(UserDefinedObject{input->data * 2});
    return true;
  }
};

void register8() {
  registerFunction<MyComplexTimesTwoFunction, Array<int64_t>, Array<int64_t>>(
      {"my_array_func"});
  registerFunction<
      MyComplexTimesTwoFunction,
      Map<int64_t, double>,
      Map<int64_t, double>>({"my_map_func"});
  registerFunction<
      MyComplexTimesTwoFunction,
      Row<int64_t, double>,
      Row<int64_t, double>>({"my_row_func"});
  registerFunction<
      MyComplexTimesTwoFunction,
      std::shared_ptr<UserDefinedObject>,
      std::shared_ptr<UserDefinedObject>>({"my_opaque_func"});
}

// Define a toy function that doubles the nested input parameters provided as an
// Array, Map, or Row type.
//
// For nested complex types, add_item() is the most common API to access the
// writer for the inner type. When adding a null entry is desired, add_null()
// for Arrays, Maps and set_null_at<index>() for Row types can be used.
template <typename TExecParams>
struct MyNestedComplexTimesTwoFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

  // This method takes and returns an Array of Maps. The return value has the
  // values of the inner map doubled. Both array and map proxy objects follow
  // their API style similar to std::vector and std::unordered_map respectively,
  // but note that they are not backed by the same. Vector elements and map
  // values are currently wrapped by std::optional to represent their
  // nullability.
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Array<Map<int64_t, double>>>& result,
      const arg_type<Array<Map<int64_t, double>>>& inputArrayOfMap) {
    result.reserve(inputArrayOfMap.size());
    for (const auto& innerMap : inputArrayOfMap) {
      // If the map entry is null, we set the output to null by setting a null
      // entry in the result array.
      // add_null() and add_item() are the APIs to be used to add an entry based
      // on its nullability.
      if (!innerMap.has_value()) {
        result.add_null();
        continue;
      }
      auto& mapWriter = result.add_item();
      for (const auto& entry : innerMap.value()) {
        mapWriter.emplace(
            entry.first * 2,
            entry.second.has_value() ? entry.second.value() * 2 : 0);
      }
    }
    return true;
  }

  // This method takes and returns a Map of integer keys and Array values. The
  // output returned has the values of the inner array doubled.
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Map<int64_t, Array<double>>>& result,
      const arg_type<Map<int64_t, Array<double>>>& inputMapOfArray) {
    result.reserve(inputMapOfArray.size());
    for (const auto& [key, arrayVal] : inputMapOfArray) {
      // add_item() supports struct binding and exposes individual writers for
      // keys and values of the map. This allows us to set them independently
      // if we have different transformation logic for keys and values.
      // Note that Velox needs the keys of the map to be non-null, so we do not
      // need to check for its nullability.
      auto [keyWriter, valueWriter] = result.add_item();
      keyWriter = key;
      if (!arrayVal.has_value()) {
        valueWriter.add_null();
        continue;
      }
      for (const auto& val : arrayVal.value()) {
        valueWriter.add_item() = val.has_value() ? val.value() * 2 : 0;
      }
    }
    return true;
  }

  // This method takes and returns a Row of two columns of type Array and Map.
  // The output returned contains the elements of the array and values of the
  // map doubled.
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Row<Array<int64_t>, Map<int64_t, double>>>& result,
      const arg_type<Row<Array<int64_t>, Map<int64_t, double>>>& inputRow) {
    const auto& elem0 = inputRow.template at<0>();
    const auto& elem1 = inputRow.template at<1>();
    // For Row types, the nullability can be set either at the "row" level or
    // at individual element.
    // In this case, we set the entire row to be null, if both the array and map
    // values are null.
    if (!elem0.has_value() && !elem1.has_value()) {
      return false;
    }

    // When only one of the elements is null and we want to set the
    // corresponding element in the output to be null, we use
    // set_null_at<index>() API.
    if (!elem0.has_value()) {
      result.template set_null_at<0>();
    } else {
      // get_writer_at<index>() API returns the value writer for the `index`
      // column in the row. This lets us set the values of individual columns
      // separately if we do not want to construct an std::tuple() to set all at
      // once.
      auto& arrayWriter = result.template get_writer_at<0>();
      arrayWriter.reserve(elem0.value().size());
      for (const auto& value : elem0.value()) {
        arrayWriter.add_item() = value.has_value() ? value.value() * 2 : 0;
      }
    }

    if (!elem1.has_value()) {
      result.template set_null_at<1>();
    } else {
      auto& mapWriter = result.template get_writer_at<1>();
      mapWriter.reserve(elem1.value().size());
      for (const auto& entry : elem1.value()) {
        mapWriter.emplace(
            entry.first * 2,
            entry.second.has_value() ? entry.second.value() * 2 : 0);
      }
    }
    return true;
  }
};

void register9() {
  registerFunction<
      MyNestedComplexTimesTwoFunction,
      Array<Map<int64_t, double>>,
      Array<Map<int64_t, double>>>({"my_nested_array_func"});
  registerFunction<
      MyNestedComplexTimesTwoFunction,
      Map<int64_t, Array<double>>,
      Map<int64_t, Array<double>>>({"my_nested_map_func"});
  registerFunction<
      MyNestedComplexTimesTwoFunction,
      Row<Array<int64_t>, Map<int64_t, double>>,
      Row<Array<int64_t>, Map<int64_t, double>>>({"my_nested_row_func"});
}

template <typename TExecParams>
struct JsonOutTest {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);
  FOLLY_ALWAYS_INLINE void call(out_type<Json>& result) {
    UDFOutputString::assign(result, "hi");
    UDFOutputString::assign(result, std::string("hi"));
    result.copy_from("hi");
    result += "hi";
    result = "hi";
  }
};

void register10() {
  registerFunction<JsonOutTest, Json>({"json_out"});
}
} // namespace

int main(int argc, char** argv) {
  // These registration functions are only split for presentation purposes;
  // one could obviously merge them into a single method.
  register1();
  register2();
  register3();
  register4();
  register5();
  register6();
  register7();
  register8();
  register9();
  register10();
  return 0;
}
