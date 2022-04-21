=============================
How to add a scalar function?
=============================

Simple Functions
----------------

This document describes the main concepts, features, and examples of the simple
function API in Velox. For more real-world API usage examples, check
**velox/example/SimpleFunctions.cpp**.

A simple scalar function, e.g. a :doc:`mathematical function </functions/math>`,
can be added by wrapping a C++ function in a templated class. For example, a
ceil function can be implemented as:

.. code-block:: c++

  template <typename TExecParams>
  struct CeilFunction {
    template <typename T>
    FOLLY_ALWAYS_INLINE void call(T& result, const T& a) {
      result = std::ceil(a);
    }
  };

All simple function classes need to be templated, and provide a "call" method
(or one of the variations described below). The top-level template parameter
provides the type system adapter, which allows developers to use non-primitive
types such as strings, arrays, maps, and struct (check below for examples).
Although the top-level template parameter is not used for functions operating
on primitive types, such as the one in the example above, it still needs to be
specified.

The call method itself can also be templated or overloaded to allow the
function to be called on different input types, e.g. float and double. Note
that template instantiation will only happen during function registration,
described in the "Registration" section below.

Please avoid using the obsolete VELOX_UDF_BEGIN/VELOX_UDF_END macros.

The "call" function (or one of its variations) may return (a) void indicating
the function never returns null values, or (b) boolean indicating whether
the result of the computation is null. True means the result is not null;
false means the result is null. If "ceil(0)" were to return null, the function
above could be re-written as follows:

.. code-block:: c++

  template <typename TExecParams>
  struct NullableCeilFunction {
    template <typename T>
    FOLLY_ALWAYS_INLINE bool call(T& result, const T& a) {
      result = std::ceil(a);
      return a != 0;
    }
  };


The argument list must start with an output parameter “result” followed by the
function arguments. The “result” argument must be a reference. Function
arguments must be const references. The C++ types of the arguments must match
Velox types as specified in the following mapping:

==========  ==============================  =============================
Velox Type  C++ Argument Type               C++ Result Type
==========  ==============================  =============================
BOOLEAN     bool                            bool
TINYINT     int8_t                          int8_t
SMALLINT    int16_t                         int16_t
INTEGER     int32_t                         int32_t
BIGINT      int64_t                         int64_t
REAL        float                           float
DOUBLE      double                          double
VARCHAR     StringView                      out_type<Varchar>
VARBINARY   StringView                      out_type<Varbinary>
TIMESTAMP   Timestamp                       Timestamp
ARRAY       arg_type<Array<E>>              out_type<Array<E>>
MAP         arg_type<Map<K,V>>              out_type<Map<K, V>>
ROW         arg_type<Row<T1, T2, T3,...>>   out_type<Row<T1, T2, T3,...>>
==========  ==============================  =============================

arg_type and out_type templates are defined by using the
VELOX_DEFINE_FUNCTION_TYPES(TExecParams) macro in the class definition. These
types provide interfaces similar to std::string, std::vector, std::unordered_map
and std::tuple. The underlying implementations are optimized to read and write
from and to the columnar representation without extra copying.

Note: Do not pay too much attention to complex type mappings at the moment.
They are included here for completeness, but require a whole separate
discussion.

Null Behavior
^^^^^^^^^^^^^

Most functions have default null behavior, e.g. a null value in any of the
arguments produces a null result. The expression evaluation engine
automatically produces nulls for such inputs, eliding a call to the actual
function. If a given function has a different behavior for null inputs, it
must define a “callNullable” function instead of a “call” function. Here is
an artificial example of a ceil function that returns 0 for null input:

.. code-block:: c++

  template <typename TExecParams>
  struct CeilFunction {
    template <typename T>
    FOLLY_ALWAYS_INLINE void callNullable(T& result, const T* a) {
      // Return 0 if input is null.
      if (a) {
        result = std::ceil(*a);
      } else {
        result = 0;
      }
    }
  };

Notice that callNullable function takes arguments as raw pointers and not
references to allow for specifying null values. callNullable() can also return
void to indicate that the function does not produce null values.

Null-Free Fast Path
*******************

A "callNullFree" function may be implemented in place of or along side "call"
and/or "callNullable" functions. When only the "callNullFree" function is
implemented, evaluation of the function will be skipped and null will
automatically be produced if any of the input arguments are null (like deafult
null behavior) or if any of the input arguments are of a complex type and
contain null anywhere in their value, e.g. an array that has a null element.
If "callNullFree" is implemented alongside "call" and/or "callNullable", an
O(N * D) check is applied to the batch to see if any of the input arguments
may be or contain null, where N is the number of input arguments and D is the
depth of nesting in complex types. Only if it can definitively be determined
that there are no nulls will "callNullFree" be invoked.  In this case,
"callNullFree" can act as a fast path by avoiding any per row null checks.

Here is an example of an array_min function that returns the minimum value in
an array:

.. code-block:: c++

  template <typename TExecParams>
  struct ArrayMinFunction {
    VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

    template <typename TInput>
    FOLLY_ALWAYS_INLINE bool callNullFree(
        TInput& out,
        const null_free_arg_type<Array<TInput>>& array) {
      out = INT32_MAX;
      for (auto i = 0; i < array.size(); i++) {
        if (array[i] < out) {
          out = array[i]
        }
      }
      return true;
    }
  };

Notice that we can access the elements of "array" without checking their
nullity in "callNullFree". Also notice that we wrap the input type in the
null_free_arg_type<...> template instead of the arg_type<...> template. This is
required as the input types for complex types are of a different type in
"callNullFree" functions that do not wrap values in an std::optional-like
interface upon access.

Determinism
^^^^^^^^^^^

By default simple functions are assumed to be deterministic, e.g. given the
same inputs they always produce the same results. If this is not the case,
the function must define a static constexpr bool is_deterministic member:

.. code-block:: c++

  static constexpr bool is_deterministic = false;

An example of such function is rand():

.. code-block:: c++

  template <typename TExecParams>
  struct RandFunction {
    static constexpr bool is_deterministic = false;

    FOLLY_ALWAYS_INLINE bool call(double& result) {
      result = folly::Random::randDouble01();
      return true;
    }
  };

All-ASCII Fast Path
^^^^^^^^^^^^^^^^^^^

Functions that process string inputs must work correctly for UTF-8 inputs.
However, these functions often can be implemented more efficiently if input is
known to contain only ASCII characters. Such functions can provide a “call”
method to process UTF-8 strings and a “callAscii” method to process ASCII-only
strings. The engine will check the input strings and invoke “callAscii” method
if input is all ASCII or “call” if input may contain multi-byte characters.

In addition, most functions that take string inputs and produce a string output
have so-called default ASCII behavior, e.g. all-ASCII input guarantees
all-ASCII output. If that’s the case, the function can indicate so by defining
the is_default_ascii_behavior member variable and initializing it to true. The
engine will automatically mark the result strings as all-ASCII. When these
strings are passed as input to some other function, the engine won’t need to
scan the strings to determine whether they are ASCII or not.

Here is an example of a trim function:

.. code-block:: c++

  template <typename TExecParams>
  struct TrimFunction {
    VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

    // ASCII input always produces ASCII result.
    static constexpr bool is_default_ascii_behavior = true;

    // Properly handles multi-byte characters.
    FOLLY_ALWAYS_INLINE bool call(
        out_type<Varchar>& result,
        const arg_type<Varchar>& input) {
      stringImpl::trimUnicodeWhiteSpace<leftTrim, rightTrim>(result, input);
      return true;
    }

    // Assumes input is all ASCII.
    FOLLY_ALWAYS_INLINE bool callAscii(
        out_type<Varchar>& result,
        const arg_type<Varchar>& input) {
      stringImpl::trimAsciiWhiteSpace<leftTrim, rightTrim>(result, input);
      return true;
    }
  };

Zero-copy String Result
^^^^^^^^^^^^^^^^^^^^^^^

Functions like :func:`substr` and :func:`trim` can produce zero-copy results by
referencing input strings. To do that they must define a reuse_strings_from_arg
member variable and initialize it to the index of the argument whose strings
are being re-used in the result. This will allow the engine to add a reference
to input string buffers to the result vector and ensure that these buffers will
not go away prematurely. The output types can be scalar strings (varchar and
varbinaries), but also complex types containing strings, such as arrays, maps,
and rows.

.. code-block:: c++

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

Access to Session Properties and Constant Inputs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Some functions require access to session properties such as session’s timezone.
Some examples are the :func:`day`, :func:`hour`, and :func:`minute` Presto
functions. Other functions could benefit from pre-processing some of the
constant inputs, e.g. compile regular expression patterns or parse date and
time units. To get access to session properties and constant inputs the
function must define an initialize method which receives a constant reference
to QueryConfig and a list of constant pointers for each of the input arguments.
Constant inputs will have their values specified. Inputs which are not constant
will be passed as nullptr's. The signature of the initialize method is similar
to that of callNullable method with an additional first parameter const
core::QueryConfig&. The engine calls the initialize method once per query and
thread of execution.

Here is an example of an hour function extracting time zone from the session
properties and using it when processing inputs.

.. code-block:: c++

  template <typename TExecParams>
  struct HourFunction {
    VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

    const date::time_zone* timeZone_ = nullptr;

    FOLLY_ALWAYS_INLINE void initialize(
        const core::QueryConfig& config,
        const arg_type<Timestamp>* /*timestamp*/) {
      timeZone_ = getTimeZoneFromConfig(config);
    }

    FOLLY_ALWAYS_INLINE bool call(
        int64_t& result,
        const arg_type<Timestamp>& timestamp) {
      int64_t seconds = getSeconds(timestamp, timeZone_);
      std::tm dateTime;
      gmtime_r((const time_t*)&seconds, &dateTime);
      result = dateTime.tm_hour;
      return true;
    }
  };

Here is another example of the :func:`date_trunc` function parsing the constant
unit argument during initialize and re-using parsed value when processing
individual rows.

.. code-block:: c++

  template <typename TExecParams>
  struct DateTruncFunction {
    VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

    const date::time_zone* timeZone_ = nullptr;
    std::optional<DateTimeUnit> unit_;

    FOLLY_ALWAYS_INLINE void initialize(
        const core::QueryConfig& config,
        const arg_type<Varchar>* unitString,
        const arg_type<Timestamp>* /*timestamp*/) {
      timeZone_ = getTimeZoneFromConfig(config);
      if (unitString != nullptr) {
        unit_ = fromDateTimeUnitString(*unitString);
      }
    }

    FOLLY_ALWAYS_INLINE bool call(
        out_type<Timestamp>& result,
        const arg_type<Varchar>& unitString,
        const arg_type<Timestamp>& timestamp) {
      const auto unit =
          unit_.has_value() ? unit_.value() : fromDateTimeUnitString(unitString);
      ...<use unit enum>...
    }
  };

Registration
^^^^^^^^^^^^

Use registerFunction template to register simple functions.

.. code-block:: c++

  template <template <class> typename Func, typename TReturn, typename... TArgs>
  void registerFunction(
      const std::vector<std::string>& aliases = {},
      std::shared_ptr<const Type> returnType = nullptr)

The first template parameter is the class name, the next template parameter is
the return type, the remaining template parameters are argument types. Aliases
parameter allows developers to specify multiple names for the same function,
but each function registration needs to provide at least one name. The "ceil"
function defined above can be registered using the following function call:

.. code-block:: c++

  registerFunction<CeilFunction, double, double>({"ceil", "ceiling");

Here, we register the CeilFunction function that takes a double and returns a
double. If we want to allow the ceil function to be called on float inputs,
we need to call registerFunction again:

.. code-block:: c++

  registerFunction<CeilFunction, float, float>({"ceil", "ceiling");

We need to call registerFunction for each signature we want to support.

Codegen
^^^^^^^

To allow the function to be used in the codegen, extract the “kernel” of the
function into a header file and call that from the “call” or “callNullable”.
Here is an example with ceil function.

.. code-block:: c++

  #include "velox/functions/prestosql/ArithmeticImpl.h"

  template <typename TExecParams>
  struct CeilFunction {
    template <typename T>
    FOLLY_ALWAYS_INLINE bool call(T& result, const T& a) {
      result = ceil(a);
      return true;
    }
  };

velox/functions/prestosql/ArithmeticImpl.h:

.. code-block:: c++

  template <typename T>
  T ceil(const T& arg) {
    T results = std::ceil(arg);
    return results;
  }

Make sure the header files that define the “kernels” are free of dependencies
as much as possible to allow for faster compilation in codegen.

Complex Types
^^^^^^^^^^^^^

Complex types as inputs
***********************
Input complex types are represented in the simple function interface using light-weight lazy
access abstractions that enable efficient direct access to the underlying data in Velox
vectors.
As mentioned earlier, the helper alias arg_type can be used in the function signature to
map Velox types to the corresponding input types. The table below shows the actual types that are
used to represent inputs of different complex types.

==========  ==============================  =============================
Velox Type  C++ Argument Type               C++ Actual Argument Type
==========  ==============================  =============================
ARRAY       arg_type<Array<E>>              ArrayView<VectorOptionalValueAccessor<VectorReader<E>>>>
MAP         arg_type<Map<K,V>>              MapView<arg_type<K>, VectorOptionalValueAccessor<VectorReader<V>>>
ROW         arg_type<Row<T1, T2, T3,...>>   RowView<VectorOptionalValueAccessor<arg_type<T1>>...>>
==========  ==============================  =============================

The view types are designed to have interfaces similar to those of std::containers, in fact in most cases
they can be used as a drop in replacement. The table below shows the mapping between the Velox type and
the corresponding std type. For example: a *Map<Row<int, int>, Array<float>>* corresponds to const
*std::map<std:::tuple<int, int>, std::vector<float>>*.

All views types are cheap to copy objects, for example the size of ArrayView is 16 bytes at max.

===========      ======================================
Lazy Input       Corresponding `std` type
===========      ======================================
ArrayView        const std::vector<std::optional<V>>
MapView          const std::map<K, std::optional<V>>
RowView          const std::tuple<std::optional<T1>...>
===========      ======================================



**1- VectorOptionalValueAccessor<VectorReader<E>>**:

VectorOptionalValueAccessor is an *std::optional* like object that provides lazy access to the nullity and
value of the underlying Velox vector at a specific index. Currently, it is used to represent elements of input arrays
and values in the input maps. Note that keys in the map are assumed to be not nullable in Velox.

The object supports the following methods:

- arg_type<E> value()      : unchecked access to the underlying value.

- arg_type<E> operator *() : unchecked access to the underlying value.

- bool has_value()         : return true if the value is not null.

- bool operator()          : return true if the value is not null.

The nullity and the value accesses are decoupled, and hence if someone knows inputs are null-free,
accessing the value does not have the overhead of checking the nullity. So is checking the nullity.
Note that, unlike std::container, function calls to value() and operator* are r-values (temporaries) and not l-values,
they can bind to const references and l-values but not references.

VectorOptionalValueAccessor<VectorReader<E>> is assignable to and comparable with std::optional<arg_type<E>>.
The following expressions are valid, where array[0] is an optional accessor.

.. code-block:: c++

    std::optional<int> = array[0];
    if(array[0] == std::nullopt) ...
    if(std::nullopt == array[0]) ...
    if(array[0]== std::optional<int>{1}) ...

**2- ArrayView<T>**:

ArrayView have an interface similar to that of const *std::vector<std::optional<V>>*, the code
below shows the function arraySum, a range loop is used to iterate over the values.

.. code-block:: c++

  template <typename T>
  struct ArraySum {
    VELOX_DEFINE_FUNCTION_TYPES(T);

    bool call(const int64_t& output, const arg_type<Array<int64_t>>& array) {
      output = 0;
      for(const auto& element : array) {
        if (element.has_value()) {
          output += element.value();
        }
      }
      return true;
    }
  };


ArrayView supports the following:

- size_t size() : return the number of elements in the array.

- VectorOptionalValueAccessor<arg_type<T>> operator[](size_t index) : access element at index.

- ArrayView<T>::Iterator begin() : iterator to the first element.

- ArrayView<T>::Iterator end() : iterator indicating end of iteration.

- bool mayHaveNulls() : constant time check on the underlying vector nullity. When it returns false, there are definitely no nulls, a true does not guarantee null existence.

- ArrayView<T>::SkipNullsContainer SkipNulls() : return an iterable container that provides direct access to non-null values in the underlying array. For example, the function above can be written as:

.. code-block:: c++

  template <typename T>
  struct ArraySum {
    VELOX_DEFINE_FUNCTION_TYPES(T);

    bool call(const int64_t& output, const arg_type<Array<int64_t>>& array) {
      output = 0;
      for (const auto& value : array.skipNulls()) {
        output += value;
      }
      return true;
    }
  };

The skipNulls iterator will check the nullity at each index and skip nulls, a more performant implementation
would skip reading the nullity when mayHaveNulls() is false.

.. code-block:: c++

  template <typename T>
  struct ArraySum {
      VELOX_DEFINE_FUNCTION_TYPES(T);

      bool call(const int64_t& output, const arg_type<Array<int64_t>>& array) {
        output = 0;
        if (array.mayHaveNulls()) {
          for(const auto& value : array.skipNulls()) {
            output += value;
          }
          return true;
        }

        // No nulls, skip reading nullity.
        for (const auto& element : array) {
          output += element.value();
        }
        return true;
      }
  };

Note: calls to operator[], iterator de-referencing, and iterator pointer de-referencing are r-values (temporaries),
versus l-values in STD containers. Hence those can be bound to const references or l-values but not normal references.

**3- MapView<K, V>**:

MapView has an interface similar to std::map<K, std::optional<V>>,  the code below shows an example function mapSum,
that sums up the keys and values.

.. code-block:: c++

  template <typename T>
  struct MapSum{
    bool call(const int64_t& output, const arg_type<Map<int64_t, int64_t>>& map) {
      output = 0;
      for (const auto& [key, value] : map) {
        output += key;
        if (value.has_value()) {
          value += value.value();
        }
      }
      return true;
    }
  };

MapView supports the following:

- MapView<K,V>::Element begin() : iterator to the first map element.

- MapView<K,V>::Element end()   : iterator that indicates end of iteration.

- size_t size()                 : number of elements in the map.

- MapView<K,V>::Iterator find(const arg_type<K>& key): performs a linear search for the key, and returns iterator to the
element if found otherwise returns end().

- MapView<K,V>::Iterator operator[](const arg_type<K>& key): same as find, throws an exception if element not found.

- MapView<K,V>::Element

MapView<K, V>::Element is the type returned by dereferencing MapView<K, V>::Iterator. It has two members:

- first : arg_type<K>

- second: VectorOptionalValueAccessor<V>.

- MapView<K, V>::Element participates in struct binding: auto [v, k] = *map.begin();

Note: iterator de-referencing and iterator pointer de-referencing result in temporaries. Hence those can be bound to
const references or value variables but not normal references.


**Temporaries lifetime C++**

While c++ allows temporaries(r-values) to bound to const references by extending their lifetime, one must be careful and
know that only the assigned temporary lifetime is extended but not all temporaries in the RHS expression chain.
In other words, the lifetime of any temporary within an expression is not extended.

For example, for the expression const auto& x = map.begin()->first.
c++ does not extend the lifetime of the result of map.begin() since it's not what is being
assigned. And in such a case, the assignment has undefined behavior.

.. code-block:: c++

     // Safe assignments. single rhs temporary.
     const auto& a = array[0];
     const auto& b = *a;
     const auto& c = map.begin();
     const auto& d = c->first;

     // Unsafe assignments. (undefined behaviours)
     const auto& a = map.begin()->first;
     const auto& b = **it;

     // Safe and cheap to assign to value.
     const auto a = map.begin()->first;
     const auto b = **it;

Note that in the range-loop, the range expression is assigned to a universal reference. Thus, the above concern applies to it.

.. code-block:: c++

     // Unsafe range loop.
     for(const auto& e : **it){..}

     // Safe range loop.
     auto itt = *it;
     for(const auto& e : *itt){..}


Limitations
***********

1. It is not possible to define functions that accept generic arrays,
maps or structs (e.g. map_keys, map_values, array_distinct, array_sort) as
it requires a generic representation for the input type that is still not
supported.

2. Output complex types now are double materialized; first in the simple functions when they
are created and then when they are copied again to the Velox vector. Some work is planned to
avoid that by using writer proxies that write directly to Velox vectors. This section will
be updated once the new writer interfaces are completed.

Variadic Arguments
^^^^^^^^^^^^^^^^^^

The last argument to a simple function may be marked "Variadic". This means
invocations of this function may include 0..N arguments of that type at the end
of the call.  While not a true type in Velox, "Variadic" can be thought of as a
syntactic type, and behaves somewhat similarly to Array.

==========  ==============================  =============================
Velox Type  C++ Argument Type               C++ Actual Argument Type
==========  ==============================  =============================
VARIADIC    arg_type<Variadic<E>>           VariadicView<VectorOptionalValueAccessor<VectorReader<E>>>>
==========  ==============================  =============================

Like the ArrayView, VariadicView has a similar interface to
*const std::vector<std::optional<V>>*.

VariadicView supports the following:

- size_t size() : return the number of arguments that were passed as part of the "Variadic" type in the function invocation.

- VectorOptionalValueAccessor<arg_type<T>> operator[](size_t index) : access the value of the argument at index.

- VariadicView<T>::Iterator begin() : iterator to the first argument.

- VariadicView<T>::Iterator end() : iterator indicating end of iteration.

- bool mayHaveNulls() : a check on the nullity of the arugments (note this takes time proportional to the number of arguments). When it returns false, there are definitely no nulls, a true does not guarantee null existence.

- VariadicView<T>::SkipNullsContainer SkipNulls() : return an iterable container that provides direct access to each argument with a non-null value.

The code below shows an example of a function that concatenates a variable number of strings:

.. code-block:: c++

     template <typename T>
     struct VariadicArgsReaderFunction {
       VELOX_DEFINE_FUNCTION_TYPES(T);

       FOLLY_ALWAYS_INLINE bool call(
           out_type<Varchar>& out,
           const arg_type<Variadic<Varchar>>& inputs) {
         for (const auto& input : inputs) {
           if (input.has_value()) {
             output += input.value();
           }
         }

         return true;
       }
     };

Vector Functions
----------------

Simple functions process a single row and produce a single value as a result.
Vector functions process a batch or rows and produce a vector of results.
Some of the defining features of these functions are:

- take vectors as inputs and produce vectors as a result;
- have access to vector encodings and metadata;
- can be defined for generic input types, e.g. generic arrays, maps and structs;
- allow for implementing :doc:`lambda functions <lambda-functions>`;

Vector function interface allows for many optimizations that are not available
to simple functions. These optimizations often leverage different vector
encodings and columnar representations of the vectors. Here are some
examples,

- :func:`map_keys` function takes advantage of the ArrayVector representation and simply returns the inner “keys” vector without doing any computation. Similarly, :func:`map_values` function simply returns the inner “values” vector.
- :func:`map_entries` function takes the pieces of the input vector - “nulls”, “sizes” and “offsets”  buffers and “keys” and “values” vectors - and simply repackages them in the form of a RowVector.
- :func:`cardinality` function takes advantage of the ArrayVector and MapVector representations and simply returns the “sizes” buffer of the input vector.
- :func:`is_null` function copies the “nulls” buffer of the input vector, flips the bits in bulk and returns the result.
- :func:`element_at` function and subscript operator for arrays and maps use dictionary encoding to represent a subset of the input “elements” or “values” vector without copying.

To define a vector function, make a subclass of exec::VectorFunction and
implement the “apply” method.

.. code-block:: c++

        void apply(
              const SelectivityVector& rows,
              std::vector<VectorPtr>& args,
              Expr* caller,
              EvalCtx* context,
              VectorPtr* result) const

Input rows
^^^^^^^^^^

The “rows” parameter specifies the set of rows in the incoming batch to
process. This set may not include all the rows. By default, a vector function
is assumed to have the default null behavior, e.g. null in any input produces
a null result. In this case, the expression evaluation engine will exclude
rows with nulls from the “rows” specified in the call to “apply”. If a
function has a different behavior for null inputs, it must override the
isDefaultNullBehavior method to return false.

.. code-block:: c++

    bool isDefaultNullBehavior() const override {
      return false;
    }

In this case, the “rows” parameter will include rows with null inputs and the
function will need to handle these. By default, the function can assume that
all inputs are not null for all “rows".

When evaluating a function as part of a conditional expression, e.g. AND, OR,
IF, SWITCH, the set of “rows” represents a subset of the rows that need
evaluating. Consider some examples.

.. code-block:: c++

    a > 5 AND b > 7

Here, a > 5 is evaluated on all rows where “a” is not null, but b > 7 is
evaluated on rows where b is not null and a is either null or not > 5.

.. code-block:: c++

    IF(condition, a + 5, b - 3)

Here, a + 5 is evaluated on rows where a is not null and condition is true,
while b - 3 is evaluated on rows where b is not null and condition is not
true.

In some cases, the values outside of “rows” may be undefined, uninitialized or
contain garbage. This would be the case if an earlier filter operation
produced dictionary-encoded vectors with indices pointing to a subset of the
rows which passed the filter. When evaluating f(g(a)), where a = Dict
(a0), function “g” is evaluated on a subset of rows in “a0” and may produce a
result where only that subset of rows is populated. Then, function “f” is
evaluated on the same subset of rows in the result of “g”. The input to “f”
will have values outside of “rows” undefined, uninitialized or contain
garbage.

Note that SelectivityVector::applyToSelected method can be used to loop over
the specified rows in a way that’s rather similar to a standard for loop.

.. code-block:: c++

    rows.applyToSelected([&] (auto row) {
        // row is the 0-based row number
        // .... process the row
    });

Input vectors
^^^^^^^^^^^^^

The “args” parameter is an std::vector of Velox vectors containing the values
of the function arguments. These vectors are not necessarily flat and may be
dictionary or constant encoded. However, a deterministic function that takes
a single argument is guaranteed to receive its only input as a flat vector.
By default, a function is assumed to be deterministic. If that’s not the
case, the function must override isDeterministic method to return false.

.. code-block:: c++

    bool isDeterministic() const override {
      return false;
    }

Note that :ref:`decoded-vector` can be used to get a flat vector-like interface to any
vector. A helper class exec::DecodedArgs can be used to decode multiple arguments.

.. code-block:: c++

    exec::DecodedArgs decodedArgs(rows, args, context);

    auto firstArg = decodedArgs.at(0);
    auto secondArg = decodedArgs.at(1);


Result vector
^^^^^^^^^^^^^

The “result” parameter is a raw pointer to VectorPtr, which is a
std::shared_ptr to BaseVector. It can be null, may point to a scratch vector
that is maybe reusable or a partially populated vector whose contents must be
preserved.

A partially populated vector is specified when evaluating the “else” branch of
an IF. In this case, the results of the “then” branch must be preserved. This
can be easily achieved by following one of the two patterns.

Calculate the result for all or just the specified rows into a new vector,
then use EvalCtx::moveOrCopyResult method to either std::move the vector
into “result” or copy individual rows into partially populated “result”.

Here is an example of using moveOrCopyResult to implement map_keys function:

.. code-block:: c++

    void apply(
        const SelectivityVector& rows,
        std::vector<VectorPtr>& args,
        exec::Expr* /* caller */,
        exec::EvalCtx* context,
        VectorPtr* result) const override {
      auto mapVector = args[0]->as<MapVector>();
      auto mapKeys = mapVector->mapKeys();

      auto localResult = std::make_shared<ArrayVector>(
          context->pool(),
          ARRAY(mapKeys->type()),
          mapVector->nulls(),
          rows.end(),
          mapVector->offsets(),
          mapVector->sizes(),
          mapKeys,
          mapVector->getNullCount());

      context->moveOrCopyResult(localResult, rows, result);
    }

Use BaseVector::ensureWritable method to initialize “result” to a flat
uniquely-referenced vector while preserving values in rows not specified
in “rows”. Then, calculate and fill in the “rows” in “result”.
BaseVector::ensureWritable creates a new vector if “result” is null. If
result is not null, but not-flat or not singly-referenced,
BaseVector::ensureWritable creates a new vector and copies non-”rows” values
from “result” into the newly created vector. If “result” is not null and
flat, BaseVector::ensureWritable checks the inner buffers and copies these if
they are not singly referenced. BaseVector::ensureWritable also recursively
calls itself on inner vectors (elements vector for the array, keys and values
for map, fields for struct) to make sure the vector is “writable” all the way
through.

Here is an example of using BaseVector::ensureWritable to implement
cardinality function for maps:

.. code-block:: c++

    void apply(
        const SelectivityVector& rows,
        std::vector<VectorPtr>& args,
        exec::Expr* /* caller */,
        exec::EvalCtx* context,
        VectorPtr* result) const override {

      BaseVector::ensureWritable(rows, BIGINT(), context->pool(), result);
      BufferPtr resultValues =
          (*result)->as<FlatVector<int64_t>>()->mutableValues(rows.size());
      auto rawResult = resultValues->asMutable<int64_t>();

      auto mapVector = args[0]->as<MapVector>();
      auto rawSizes = mapVector->rawSizes();

      rows.applyToSelected([&](vector_size_t row) {
        rawResult[row] = rawSizes[row];
      });
    }

Simple implementation
^^^^^^^^^^^^^^^^^^^^^

Vector function interface is very flexible and allows for many interesting
optimizations. It may also feel very complicated. Let’s see how we can use
DecodedVector and BaseVector::ensureWritable to implement the “power(a, b)”
function as a vector function in a way that is not much more complicated than
the simple function. To clarify, it is best to implement the “power” function
as a simple function. I’m using it here for illustration purposes only.

.. code-block:: c++

    // Initialize flat results vector.
    BaseVector::ensureWritable(rows, DOUBLE(), context->pool(), result);
    auto rawResults = (*result)->as<FlatVector<int64_t>>()->mutableRawValues();

    // Decode the arguments.
    DecodedArgs decodedArgs(rows, args, context);
    auto base = decodedArgs.decodedVector(0);
    auto exp = decodedArgs.decodedVector(1);

    // Loop over rows and calculate the results.
    rows.applyToSelected([&](int row) {
      rawResults[row] =
          std::pow(base->valueAt<double>(row), exp->valueAt<double>(row));
    });

You may want to optimize for the case when both base and exponent being flat
and eliminate the overhead of calling DecodedVector::valueAt template.

.. code-block:: c++

    if (base->isIdentityMapping() && exp->isIdentityMapping()) {
      auto baseValues = base->values<double>();
      auto expValues = exp->values<double>();
      rows.applyToSelected([&](int row) {
        rawResults[row] = std::pow(baseValues[row], expValues[row]);
      });
    } else {
      rows.applyToSelected([&](int row) {
        rawResults[row] =
            std::pow(base->valueAt<double>(row), exp->valueAt<double>(row));
      });
    }

You may decide to further optimize for the case of flat base and constant
exponent.

.. code-block:: c++

    if (base->isIdentityMapping() && exp->isIdentityMapping()) {
      auto baseValues = base->values<double>();
      auto expValues = exp->values<double>();
      rows.applyToSelected([&](int row) {
        rawResults[row] = std::pow(baseValues[row], expValues[row]);
      });
    } else if (base->isIdentityMapping() && exp->isConstantMapping()) {
      auto baseValues = base->values<double>();
      auto expValue = exp->valueAt<double>(0);
      rows.applyToSelected([&](int row) {
        rawResults[row] = std::pow(baseValues[row], expValue);
      });
    } else {
      rows.applyToSelected([&](int row) {
        rawResults[row] =
            std::pow(base->valueAt<double>(row), exp->valueAt<double>(row));
      });
    }

Hopefully, you can see now that additional complexity in the implementation
comes only from introducing optimization paths. Developers need to decide
whether that complexity is justified on a case by case basis.

TRY expression support
^^^^^^^^^^^^^^^^^^^^^^

A built-in TRY expression evaluates input expression and handles certain types
of errors by returning NULL. It is used for the cases where it is preferable
that queries produce NULL or default values instead of failing when corrupt
or invalid data is encountered. To specify default values, the TRY expression
can be used in conjunction with the COALESCE function.

The implementation of the TRY expression relies on the VectorFunction
implementation to call EvalCtx::setError(row, exception) instead of throwing
exceptions directly.

.. code-block:: c++

    void setError(vector_size_t index, const std::exception_ptr& exceptionPtr);

A typical pattern would be to loop over rows, apply a function wrapped in a
try-catch and call context->setError(row, std::current_exception()); from the
catch block.

.. code-block:: c++

    rows.applyToSelected([&](auto row) {
      try {
        // ... calculate and store the result for the row
      } catch (const std::exception& e) {
        context->setError(row, std::current_exception());
      }
    });

There is an EvalCtx::applyToSelectedNoThrow convenience method that can be used
instead of the explicit try-catch block above:

.. code-block:: c++

    context->applyToSelectedNoThrow(rows, [&](auto row) {
      // ... calculate and store the result for the row
    });


Simple functions are compatible with the TRY expression by default. The framework
wraps the “call” and “callNullable” methods in a try-catch and reports errors
using context->setError.

Registration
^^^^^^^^^^^^

Use exec::registerVectorFunction to register a stateless vector function.

.. code-block:: c++

    bool registerVectorFunction(
        const std::string& name,
        std::vector<FunctionSignaturePtr> signatures,
        std::unique_ptr<VectorFunction> func,
        bool overwrite = true)

exec::registerVectorFunction takes a name, a list of supported signatures
and unique_ptr to an instance of the function. An optional “overwrite” flag
specifies whether to overwrite a function if a function with the specified
name already exists.

Use exec::registerStatefulVectorFunction to register a stateful vector
function.

.. code-block:: c++

    bool registerStatefulVectorFunction(
        const std::string& name,
        std::vector<FunctionSignaturePtr> signatures,
        VectorFunctionFactory factory,
        bool overwrite = true)

exec::registerStatefulVectorFunction takes a name, a list of supported
signatures and a factory function that can be used to create an instance of
the vector function. Expression evaluation engine uses a factory function to
create a new instance of the vector function for each thread of execution. In
a single-threaded execution, a single instance of the function is used to
process all batches of data. In a multi-threaded execution, each thread makes
a separate instance of the function.

Factory function is called with a function name, types and optionally constant
values for the arguments. For example, regular expressions functions are
often called with constant regular expressions. A stateful vector function
can compile the regular expression once (per thread of execution) and reuse
the compiled expression for multiple batches of data. Similarly, an IN
expression used with a constant IN-list can create a hash set of the values
once and reuse it for all the batches of data.

.. code-block:: c++

    // Represents arguments for stateful vector functions. Stores element type, and
    // the constant value (if supplied).
    struct VectorFunctionArg {
      const TypePtr type;
      const VectorPtr constantValue;
    };

    using VectorFunctionFactory = std::function<std::shared_ptr<VectorFunction>(
        const std::string& name,
        const std::vector<VectorFunctionArg>& inputArgs)>;

.. _function-signature:

Function signature
^^^^^^^^^^^^^^^^^^

It is recommended to use FunctionSignatureBuilder to create FunctionSignature
instances. FunctionSignatureBuilder and FunctionSignature support Java-like
generics, variable number of arguments and lambdas. Here are some examples.

The length function takes a single argument of type varchar and returns a
bigint:

.. code-block:: c++

    // varchar -> bigint
    exec::FunctionSignatureBuilder()
      .returnType("bigint")
      .argumentType("varchar")
      .build()

The substr function takes a varchar and two integers for start and length. To
specify types of multiple arguments, call argumentType() method for each
argument in order.

.. code-block:: c++

    // varchar, integer, integer -> bigint
    exec::FunctionSignatureBuilder()
      .returnType("varchar")
      .argumentType("varchar")
      .argumentType("integer")
      .argumentType("integer")
      .build()

The concat function takes an arbitrary number of varchar inputs and returns a
varchar. FunctionSignatureBuilder allows specifying that the last augment may
appear zero or more times by calling variableArity() method.

.. code-block:: c++

    // varchar... -> varchar
    exec::FunctionSignatureBuilder()
        .returnType("varchar")
        .argumentType("varchar")
        .variableArity()
        .build()

The map_keys function takes any map and returns an array of map keys.

.. code-block:: c++

    // map(K,V) -> array(K)
    exec::FunctionSignatureBuilder()
      .typeVariable("K")
      .typeVariable("V")
      .returnType("array(K)")
      .argumentType("map(K,V)")
      .build()

The transform function takes an array and a lambda, applies the lambda to each
element of the array and returns a new array of the results.

.. code-block:: c++

    // array(T), function(T, U) -> array(U)
    exec::FunctionSignatureBuilder()
      .typeVariable("T")
      .typeVariable("U")
      .returnType("array(U)")
      .argumentType("array(T)")
      .argumentType("function(T, U)")
      .build();

The type names used in FunctionSignatureBuilder can be either lowercase
standard types, a special type “any”, or the ones defined by calling
typeVariable() method. “any” type can be used to specify a printf-like
function which takes any number of arguments of any possibly non-matching
types.

Testing
-------

Add a test using FunctionBaseTest from
velox/functions/prestosql/tests/FunctionBaseTest.h as a base class. Name your test
and the .cpp file <function-name>Test, e.g. CardinalityTest in
CardinalityTest.cpp or IsNullTest in IsNullTest.cpp.

FunctionBaseTest has many helper methods for generating test vectors. It also
provides an evaluate() method that takes a SQL expression and input data,
evaluates the expression and returns the result vector. SQL expression is
parsed using DuckDB and type resolution logic is leveraging the function
signatures specified during registration. assertEqualVectors() method takes
two vectors, expected and actual, and asserts that they represent the same
values. The encodings of the vectors may not be the same.

Here is an example of a test for vector function “contains”:

.. code-block:: c++

    TEST_F(ArrayContainsTest, integerWithNulls) {
      auto arrayVector = makeNullableArrayVector<int64_t>(
          {{1, 2, 3, 4},
           {3, 4, 5},
           {},
           {5, 6, std::nullopt, 7, 8, 9},
           {7, std::nullopt},
           {10, 9, 8, 7}});

      auto testContains = [&](std::optional<int64_t> search,
                              const std::vector<std::optional<bool>>& expected) {
        auto result = evaluate<SimpleVector<bool>>(
            "contains(c0, c1)",
            makeRowVector({
                arrayVector,
                makeConstant(search, arrayVector->size()),
            }));

        assertEqualVectors(makeNullableFlatVector<bool>(expected), result);
      };

      testContains(1, {true, false, false, std::nullopt, std::nullopt, false});
      testContains(3, {true, true, false, std::nullopt, std::nullopt, false});
      testContains(5, {false, true, false, true, std::nullopt, false});
      testContains(7, {false, false, false, true, true, true});
      testContains(-2, {false, false, false, std::nullopt, std::nullopt, false});
    }

Tests for simple functions could benefit from using the evaluateOnce
() template which takes SQL expression and scalar values for the inputs,
evaluates the expression on a vector of length 1 and returns the scalar
result. Here is an example of a test for simple function “sqrt”:

.. code-block:: c++

    TEST_F(ArithmeticTest, sqrt) {
      constexpr double kDoubleMax = std::numeric_limits<double>::max();
      const double kNan = std::numeric_limits<double>::quiet_NaN();

      const auto sqrt = [&](std::optional<double> a) {
        return evaluateOnce<double>("sqrt(c0)", a);
      };

      EXPECT_EQ(1.0, sqrt(1));
      EXPECT_TRUE(std::isnan(sqrt(-1.0).value_or(-1)));
      EXPECT_EQ(0, sqrt(0));

      EXPECT_EQ(2, sqrt(4));
      EXPECT_EQ(3, sqrt(9));
      EXPECT_FLOAT_EQ(1.34078e+154, sqrt(kDoubleMax).value_or(-1));
      EXPECT_EQ(std::nullopt, sqrt(std::nullopt));
      EXPECT_TRUE(std::isnan(sqrt(kNan).value_or(-1)));
    }

Benchmarking
------------

Add a benchmark using folly::Benchmark framework and FunctionBenchmarkBase
from velox/functions/lib/benchmarks/FunctionBenchmarkBase.h as a base class.
Benchmarks are a great way to check if an optimization is working, evaluate
how much benefit it brings and decide whether it is worth the additional
complexity.

Documenting
-----------

If a function implements Presto semantics, document it by adding an entry to
one of the `*.rst` files in velox/docs/functions. Each file documents a set of
related functions. E.g. math.rst contains all of the mathematical functions,
while array.rst file contains all of the array functions. Within a file,
functions are listed in alphabetical order.
