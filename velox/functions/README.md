# Velox Functions

There are different ways to define new UDF functions in Velox.

## Scalar Functions

The most straight forward way to add new UDFs to Velox is to add them as Scalar function and then declare and register
them in Velox system. For example all arithmetic functions in Velox are scalar functions. For example here is how the
`round` function is defined using templates (result first followed by arguments) in `velox/functions/prestosql/Arithmetic.h`.

```c++
template <typename T>
VELOX_UDF_BEGIN(round)
FOLLY_ALWAYS_INLINE bool call(T& result, const T& a, const int32_t b = 0) {
  result = round(a, b);
  return true;
}
VELOX_UDF_END();
```

`T` is the cpp type equivalent of Velox type of the input and output arguments in this case.

The round function is registered for different acceptable input types in `function/common/RegisterArithmetic.cpp`:

```c++
...
  registerUnaryNumeric<udf_round>({"round"});
  registerFunction<udf_round<int8_t>, int8_t, int8_t, int32_t>({"round"});
  registerFunction<udf_round<int16_t>, int16_t, int16_t, int32_t>({"round"});
  registerFunction<udf_round<int32_t>, int32_t, int32_t, int32_t>({"round"});
  registerFunction<udf_round<int64_t>, int64_t, int64_t, int32_t>({"round"});
  registerFunction<udf_round<double>, double, double, int32_t>({"round"});
  registerFunction<udf_round<float>, float, float, int32_t>({"round"});
...
```

Note the symbol udf_round is automatically generated from the symbol round in the original definition.

## Optimizing String Functions for ASCII

For functions dealing with string inputs and outputs, please use the mechanisms provided for optimizing for ASCII and
Unicode cases. For example here's how upper function is implemented `functions/lib/string/StringImpl.h`

By passing asciiness via the template variable we know the string is ascii and so run the extremely simpler version of the
function compared to unicode (utf8) implementation using `utf8proc` library. Note that the template variabe `ascii` having
value `true` indicates that the vector has no unicode character in it.

```c++
/// Perform upper for a UTF8 string
template <bool ascii, typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE bool upper(TOutString& output, const TInString& input) {
  if constexpr (ascii) {
    output.resize(input.size());
    upperAscii(output.data(), input.data(), input.size());
  } else {
    output.resize(input.size() * 4);
    auto size =
        upperUnicode(output.data(), output.size(), input.data(), input.size());
    output.resize(size);
  }
  return true;
}

FOLLY_ALWAYS_INLINE static void
upperAscii(char* output, const char* input, size_t length) {
  for (auto i = 0; i < length; i++) {
    if (input[i] >= 'a' && input[i] <= 'z') {
      output[i] = input[i] - 32;
    } else {
      output[i] = input[i];
    }
  }
}

FOLLY_ALWAYS_INLINE size_t upperUnicode(
    char* output,
    size_t outputLength,
    const char* input,
    size_t inputLength) {
  auto inputIdx = 0;
  auto outputIdx = 0;

  while (inputIdx < inputLength) {
    utf8proc_int32_t nextCodePoint;
    int size;
    nextCodePoint = utf8proc_codepoint(&input[inputIdx], size);
    inputIdx += size;

    auto newSize = utf8proc_encode_char(
        utf8proc_toupper(nextCodePoint),
        reinterpret_cast<unsigned char*>(&output[outputIdx]));
    outputIdx += newSize;
    assert(outputIdx < outputLength && "access out of bound");
  }
  return outputIdx;
}
```

## Vector Functions

Vector functions are usually used in cases in which performance optimizations are necessary per batch such as

* when dealing with complex types such as strings, struct, maps and arrays (functions such as element_at, substr,
  coalese)
* when we can perform optimizations for a constant input (functions such as regex).

The best way to implement a function as a vector function is usually to follow a specific template. A full description
of that template and its steps is given in details in `functions/common/VectorArithmetic.cpp`

## Stateful Vector Functions

Stateful vector functions are function objects that can keep some state many runs of vector batches in the query so they
can amortize the initialization cost even more than normal vector function which can hold state over each single input
batch. One example of these functions is IN predicate in SQL where the list of items in IN expression are all given and
can can be pre-processed. You can find the source in `functions/common/InPredicate.cpp`

In addition to template pieces for normal Vector functions, the stateful predicate function need to provide a `create`
function which passed to the their registration function. For example for `IN`:

```c++
// Can be shared across functions. However, you will want to control which types the function is actually instantiated for, so there are limits to what can be shared.
template <typename Function, typename... Args>
std::shared_ptr<VectorFunction> createWithType(TypeKind kind, Args&&... args) {
  switch (kind) {
    case INTEGER: return std::make_shared<Function<int32_t>>(std::forward<Args>(args)...);
    ...
  }
  return nullptr;
}

namespace {
template <TypeKind kind>
class InPredicate final : public VectorFunction {
    using T = Traits<kind>::CppType;

    InPredicate(inputTypes, constantInputs) {
      for (int i = 1; i < constantInputs.size(); ++i) {
        if (!constantInputs[i]) throw unimplemented();
        else if (constantInputs[i]->isNullAt(0)) has_null_ = true;
        else if (auto converted = tryConvert<kind>(constantInputs[i]); converted && *converted == *converted)
          elements_.emplace(*converted);
        // Otherwise, out of range or cannot match.
      }
    }

    void apply(....) override {
      ...
      rows.applyToSelected([&](int i) {
        bool found = elements_.contains(lhs->valueAt<T>(i));
        // Possibly worth promoting has_null to a template argument.
        if (!has_null_) result.set(i, found);
        else found ? result.setNull(i, true) : result.set(i, false);
      });
    }

    void inferTypes() override {
       // There's currently a bunch of logic here to check for type compatibility, but that can be checked in the factory function below instead.
      return BOOLEAN();
    }

    bool has_null_ = false;
    some_fast_hash_set<T> elements_;
};

} // namespace
```

Here the create function returns the final vector function object optimized for the inputs.
`constantInputs` holds the values of constant inputs (literals) to the function (nullptr for the arguments that are
literal.

Here's how the function is registered:

```c++
VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(udf_in, InPredicate::create);
```

## Aggregate Functions

**TO BE COMPLETED**

## UnitTest Functions

When writing tests for functions, it is convenient to use FunctionBaseTest base class. These class has methods for
making new vectors via VectorMaker utility class. PlanBuilder is useful for building query plans. FunctionBaseTest
provides an "evaluate" method that takes a SQL expression string and an input vector, evaluates the expression and
returns the results. Here is how one can write a simple test for multiply function:

```c++
vector_size_t size = 1'000;
  auto input = makeFlatVector<int64_t>(size, [](vector_size_t row) { return row; }, nullEvery(5));

  auto expected = makeFlatVector<int64_t>(
      size, [](vector_size_t row) { return row * 2; }, nullEvery(5));
  auto result =
      evaluate<FlatVector<int64_t>>("C0 * 2", makeRowVector({input}));

  ASSERT_EQ(expected->size(), result->size());
  for (vector_size_t row = 0; row < result->size(); row++) {
    ASSERT_TRUE(expected->equalValueAt(result.get(), row, row))
        << "at " << row;
  }
```

You can find more examples in the code base by searching for FunctionBaseTest
