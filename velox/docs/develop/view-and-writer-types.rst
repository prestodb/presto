=====================
View and Writer Types
=====================

View types and writer types are used as the input and output parameter types
respectively for complex and string types in the simple function interface of
both scalar and aggregate functions.

Inputs (View Types)
-------------------

Input complex types are represented in the simple function interface using light-weight lazy
access abstractions that enable efficient direct access to the underlying data in Velox
vectors.
As mentioned earlier, the helper aliases arg_type and null_free_arg_type can be used in function's signatures to
map Velox types to the corresponding input types. The table below shows the actual types that are
used to represent inputs of different complex types.

==============================    =========================   ==============================
 C++ Argument Type                 C++ Actual Argument Type   Corresponding `std` type
==============================    =========================   ==============================
arg_type<Array<E>>                NullableArrayView<E>>       std::vector<std::optional<V>>
arg_type<Map<K,V>>                NullableMapView<K, V>       std::map<K, std::optional<V>>
arg_type<Row<T...>>               NullableRowView<T...>       std::tuple<std::optional<T>...
null_free_arg_type<Array<E>>      NullFreeArrayView<E>        std::vector<V>
null_free_arg_type<Map<K,V>>      NullFreeMapView<K, V>       std::map<K, V>
null_free_arg_type<Row<T...>>>    NullFreeRowView<T...>       std::tuple<T...>
==============================    =========================   ==============================

The view types are designed to have interfaces similar to those of std::containers, in fact in most cases
they can be used as a drop in replacement. The table above shows the mapping between the Velox type and
the corresponding std type. For example: a *Map<Row<int, int>, Array<float>>* corresponds to const
*std::map<std:::tuple<int, int>, std::vector<float>>*.

All views types are cheap to copy objects, for example the size of ArrayView is 16 bytes at max.

**OptionalAccessor<E>**:

OptionalAccessor is an *std::optional* like object that provides lazy access to the nullity and
value of the underlying Velox vector at a specific index. Currently, it is used to represent elements of nullable input arrays
and values of nullable input maps. Note that keys in the map are assumed to be always not nullable in Velox.

The object supports the following methods:

- arg_type<E> value()      : unchecked access to the underlying value.

- arg_type<E> operator \*() : unchecked access to the underlying value.

- bool has_value()         : return true if the value is not null.

- bool operator()          : return true if the value is not null.

The nullity and the value accesses are decoupled, and hence if someone knows inputs are null-free,
accessing the value does not have the overhead of checking the nullity. So is checking the nullity.
Note that, unlike std::container, function calls to value() and operator* are r-values (temporaries) and not l-values,
they can bind to const references and l-values but not references.

OptionalAccessor<E> is assignable to and comparable with std::optional<arg_type<E>> for primitive types.
The following expressions are valid, where array[0] is an optional accessor.

.. code-block:: c++

    std::optional<int> = array[0];
    if(array[0] == std::nullopt) ...
    if(std::nullopt == array[0]) ...
    if(array[0]== std::optional<int>{1}) ...

**NullableArrayView<T> and NullFreeArrayView<T>**

NullableArrayView and NullFreeArrayView have interfaces similar to that of *std::vector<std::optional<V>>* and *std::vector<V>*,
the code below shows the function arraySum, a range loop is used to iterate over the values.

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

- size_t **size** () : return the number of elements in the array.

- **operator[]** (size_t index) : access element at index. It returns either null_free_arg_type<T> or OptionalAccessor<T>.

- ArrayView<T>::Iterator **begin** () : iterator to the first element.

- ArrayView<T>::Iterator **end** () : iterator indicating end of iteration.

- bool **mayHaveNulls** () : constant time check on the underlying vector nullity. When it returns false, there are definitely no nulls, a true does not guarantee null existence.

- ArrayView<T>::SkipNullsContainer **skipNulls** () : return an iterable container that provides direct access to non-null values in the underlying array. For example, the function above can be written as:

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

**NullableMapView<K, V> and  NullFreeMapView<K, V>**

NullableMapView and NullFreeMapView has an interfaces similar to std::map<K, std::optional<V>> and std::map<K, V>,
the code below shows an example function mapSum, sums up the keys and values.

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

- MapView<K,V>::Element **begin** () : iterator to the first map element.

- MapView<K,V>::Element **end** ()   : iterator that indicates end of iteration.

- size_t **size** ()                 : number of elements in the map.

- MapView<K,V>::Iterator **find** (const key_t& key): performs a linear search for the key, and returns iterator to the element if found otherwise returns end(). Only supported for primitive key types.

- MapView<K,V>::Iterator **operator[]** (const key_t& key): same as find, throws an exception if element not found.

- MapView<K,V>::Element

MapView<K, V>::Element is the type returned by dereferencing MapView<K, V>::Iterator. It has two members:

- first : arg_type<K> | null_free_arg_type<K>

- second: OptionalAccessor<V> | null_free_arg_type<V>

- MapView<K, V>::Element participates in struct binding: auto [v, k] = \*map.begin();

Note: iterator de-referencing and iterator pointer de-referencing result in temporaries. Hence those can be bound to
const references or value variables but not normal references.

Generic<T1> input types are implemented using GenericView that supports the following:

- uint64_t **hash** () const : returns a hash of the value; used to define std::hash<GenericView>(); allows GenericView's to be stored in folly::F14 sets and maps as well as STL's sets and maps.
- bool **isNull** () const   : returns true if the value is NULL
- bool **operator==** (const GenericView& other) const : equality comparison with another GenericView
- std::optional<int64_t> **compare** (const GenericView& other, const CompareFlags flags) const : comparison with another GenericView
- TypeKind **kind** () const : returns TypeKind of the value
- const TypePtr& **type** () const : returns Velox type of the value
- std::string **toString** () const : returns string representaion of the value for logging and debugging
- template <typename ToType> typename VectorReader<ToType>::exec_in_t **castTo** () const : cast to concrete view type
- template <typename ToType> std::optional<typename VectorReader<ToType>::exec_in_t> **tryCastTo** () const : best-effort attempt to cast to a concrete view type

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

.. _outputs-write:

Outputs (Writer Types)
----------------------

Outputs of complex types are represented using special writers that are designed in a way that
minimizes data copying by writing directly to Velox vectors.

**ArrayWriter<V>**

- out_type<V>& **add_item** () : add non-null item and return the writer of the added value.
- **add_null** (): add null item.
- **reserve** (vector_size_t size): make sure space for `size` items is allocated in the underlying vector.
- vector_size_t **size** (): return the length of the array.
- **resize** (vector_size_t size): change the size of the array reserving space for the new elements if needed.

- void **add_items** (const T& data): append data from any container with std::vector-like interface.
- void **copy_from** (const T& data): assign data to match that of any container with std::vector-like interface.

- void **add_items** (const NullFreeArrayView<V>& data): append data from array view (faster than item by item).
- void **copy_from** (const NullFreeArrayView<V>& data): assign data from array view (faster than item by item).

- void **add_items** (const NullableArrayView<V>& data): append data from array view (faster than item by item).
- void **copy_from** (const NullableArrayView<V>& data): assign data from array view (faster than item by item).

When V is primitive, the following functions are available, making the writer usable as std::vector<V>.

- **push_back** (std::optional<V>): add item or null.
- PrimitiveWriter<V> **operator[]** (vector_size_t index): return a primitive writer that is assignable to std::optional<V> for the item at index (should be called after a resize).
- PrimitiveWriter<V> **back** (): return a primitive writer that is assignable to std::optional<V> for the item at index length -1.


**MapWriter<K, V>**

- **reserve** (vector_size_t size): make sure space for `size` entries is allocated in the underlying vector.
- std::tuple<out_type<K>&, out_type<V>&> **add_item()** : add non-null item and return the writers of key and value as tuple.
- out_type<K>& **add_null()** : add null item and return the key writer.
- vector_size_t **size** (): return the length of the map.

- void **add_items** (const T& data): append data from any container with std::vector<tuple<K, V>> like interface.
- void **copy_from** (const NullFreeMapView<V>& data): assign data from map view (faster than item by item).
- void **copy_from** (const NullableMapView<V>& data): assign data from map view (faster than item by item).

When K and V are primitives, the following functions are available, making the writer usable as std::vector<std::tuple<K, V>>.

- **resize** (vector_size_t size): change the size.
- **emplace** (K, std::optional<V>): add element to the map.
- std::tuple<K&, PrimitiveWriter<V>> **operator[]** (vector_size_t index): returns pair of writers for element at index. Key writer is assignable to K. while value writer is assignable to std::optional<V>.

**RowWriter<T...>**

- template<vector_size_t I> **set_null_at** (): set null for row item at index I.
- template<vector_size_t I> **get_writer_at** (): set not null for row item at index I, and return writer to the row element at index I.

When all types T... are primitives, the following functions are available.

- void **operator=** (const std::tuple<T...>& inputs): assignable to std::tuple<T...>.
- void **operator=** (const std::tuple<std::optional<T>...>& inputs): assignable to std::tuple<std::optional<T>...>.
- void **copy_from** (const std::tuple<K...>& inputs): similar as the above.

When a given Ti is primitive, the following is valid.

- PrimitiveWriter<Ti> exec::get<I>(RowWriter<T...>): return a primitive writer for item at index I that is assignable to std::optional.

**PrimitiveWriter<T>**

Assignable to std::optional<T> allows writing null or value to the primitive. Returned by complex writers when writing nullable
primitives.

**StringWriter<>**

- void **reserve** (size_t newCapacity) : Reserve a space for the output string with size of at least newCapacity.
- void **resize** (size_t newCapacity) : Set the size of the string.
- char* **data** (): returns pointer to the first char of the string, can be written to directly (safe to write to index at capacity()-1).
- vector_size_t **capacity** (): returns the capacity of the string.
- vector_size_t **size** (): returns the size of the string.
- **operator+=** (const T& input): append data from char* or any type with data() and size().
- **append** (const T& input): append data from char* or any type with data() and size().
- **copy_from** (const T& input): append data from char* or any type with data() and size().

When Zero-copy optimization is enabled (see zero-copy-string-result section above), the following functions can be used.

- void **setEmpty** (): set to empty string.
- void **setNoCopy** (const StringView& value): set string to an input string without performing deep copy.

**GenericWriter**

- TypeKind **kind** () const : returns TypeKind of the value
- const TypePtr& **type** () const : returns Velox type of the value
- void **copy_from** (const GenericView& view) : assign data from another GenericView
- template <typename ToType> typename VectorWriter<ToType, void>::exec_out_t& **castTo** () : cast to concrete writer type
- template <typename ToType> typename VectorWriter<ToType, void>::exec_out_t* **tryCastTo** () : best-effort attempt to cast to a concrete writer type

Limitations
-----------

1. If a function throws an exception while writing a complex type, then the output of the
row being written as well as the output of the next row are undefined. Hence, it's recommended
to avoid throwing exceptions after writing has started for a complex output within the function.
