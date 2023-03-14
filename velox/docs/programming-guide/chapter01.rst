===================================
Chapter 1: Buffers and Flat Vectors
===================================

Velox stores and processes data one column at a time. A column of 64-bit
integers is stored in memory using 2 contiguous buffers: a buffer of values and
a buffer of null flags. To store 100 rows, we need to allocate values
buffer that fits 100 64-bit integers: 100 * 8 = 800 bytes, and nulls buffer
that fits 100 bits: 100 / 8 = 13 bytes. Velox allocates memory using
MemoryPool.

Let’s start by getting access to a MemoryPool:

.. code-block:: c++

    #include "velox/common/memory/Memory.h"

    auto pool = memory::getDefaultMemoryPool();

`pool` is a std::shared_ptr<velox::memory::MemoryPool>. We can use it to
allocate buffers.

.. code-block:: c++

    #include "velox/buffer/Buffer.h"

    auto values = AlignedBuffer::allocate<int64_t>(100, pool.get());
    auto nulls = AlignedBuffer::allocate<bool>(100, pool.get());

AlignedBuffer::allocate is a template with a single template parameter T, which
indicates the type of values we want to store in the buffer. sizeof(T) is used
to determine how many bytes each value needs. AlignedBuffer::allocate takes
number of values (numElements) and a memory pool and allocates at least
numElement * sizeof(T) bytes, but usually more. We can find out how much memory
was allocated using capacity() method on the returned BufferPtr.

.. code-block:: c++

    template <typename T>
    static BufferPtr allocate(
       size_t numElements,
       velox::memory::MemoryPool* pool,
       const std::optional<T>& initValue = std::nullopt) {

The `values` and `nulls` variables above are instances of BufferPtr, which is a
boost::intrusive_ptr<Buffer>. Similar to `std::vector <https://en.cppreference.com/w/cpp/container/vector>`_,
a Buffer has a size and capacity. Let’s check these for the `values` and `nulls`
buffers.

.. code-block:: c++

    LOG(INFO) << values->size() << ", " << values->capacity();

    > 800, 928

    LOG(INFO) << nulls->size() << ", " << nulls->capacity();

    > 13, 32

As expected, the size of the values buffer is 800 bytes and size of the nulls
buffer is 13 bytes. The capacity of the buffers is slightly more. We can change
the size of the buffer to any value that doesn’t exceed the buffer's capacity
using Buffer::setSize() method.

.. code-block:: c++

    values->setSize(900);

    > 900, 928

    nulls->setSize(20);

    > 20, 32

Notice that setSize method takes number of bytes, not number of values of type
T. Also, Buffer itself is not a template and therefore is not aware of the type
of values that are being stored in it.

Setting the size to a value that exceeds the Buffer’s capacity results in an
error.

.. code-block:: c++

    values->setSize(1'000);

    VeloxRuntimeError
    Error Source: RUNTIME
    Error Code: INVALID_STATE
    Reason: (1000 vs. 928)
    Retriable: False
    Expression: size <= capacity_
    Function: setSize
    File: /Users/mbasmanova/cpp/velox-1/./velox/buffer/Buffer.h
    Line: 119

To read values from a buffer, call Buffer::as<T> template method that returns a
const T*. It is effectively a `reinterpret_cast <https://en.cppreference.com/w/cpp/language/reinterpret_cast>`_
of the underlying memory buffer.

.. code-block:: c++

    auto* rawValues = values->as<int64_t>();

    LOG(INFO) << rawValues[5];

    > -6799976246779207263

We allocated memory for the `values` buffer, but didn’t write any values yet,
hence, we are getting some "garbage" values when reading from the buffer.
That’s expected.

We could pass an initial value to AlignedBuffer::allocate though. Let’s allocate
the `values` buffer and initialize all values to "25".

.. code-block:: c++

    auto values = AlignedBuffer::allocate<int64_t>(100, pool.get(), 25);

    auto* rawValues = values->as<int64_t>();

    LOG(INFO) << rawValues[5];

    > 25

To write values into the allocated buffer, call Buffer::asMutable<T> template
method that returns a T*. Just like the Buffer::a<T> template, it is
effectively a reinterpret_cast of the underlying memory buffer.

Let’s populate the buffer with 100 sequential numbers starting from 0: 0, 1,
2,...99.

.. code-block:: c++

    auto* rawValues = values->asMutable<int64_t>();

    for (auto i = 0; i < 100; ++i) {
     rawValues[i] = i;
    }

    LOG(INFO) << rawValues[5];

    > 5

We could also use std::iota to populate the buffer with sequential values:

.. code-block:: c++

    std::iota(rawValues, rawValues + 100, 0);

    for (auto i = 0; i < 10; ++i) {
     LOG(INFO) << i << ": " << rawValues[i];
    }

    > 0: 0
    > 1: 1
    > 2: 2

BufferPtr is a smart pointer, so we don’t need to worry about freeing up memory.
Once the last reference goes out of scope, the Buffer object will get destroyed
calling MemoryPool to release the memory.

We do need to make sure that MemoryPool stays alive until after all buffers
allocated from it are destroyed.

Let's now look at the nulls buffer.

We use the nulls buffer to store null flags, one bit per value.
AlignedBuffer<boo>::allocate template is overwritten to allocate only one bit
per entry, not 1 byte (=sizeof(bool). To read and write null bits we use
Buffer::as<uint64_t>() and Buffer::asMutable<uint64_t>() method. Notice that we
use uint64_t as template parameter, not bool.

.. code-block:: c++

    #include "velox/common/base/Nulls.h"

    auto* rawNulls = nulls->as<uint64_t>();

    LOG(INFO) << std::boolalpha << bits::isBitNull(rawNulls, 5);

    > false

We use bits::isBitNull function to read the N-th bit of the nulls buffer and
turn it into a boolean.

We haven’t written any values into the nulls buffer and we haven’t provided an
initial value when allocating the buffer, hence, we are getting some "garbage"
values as expected.

We can pass an initial value to AllignedBuffer::allocate<bool>(): bits::kNull or
bits::kNotNull.

.. code-block:: c++

    auto nulls = AlignedBuffer::allocate<bool>(100, pool.get(), bits::kNull);

    auto* rawNulls = nulls->as<uint64_t>();

    LOG(INFO) << std::boolalpha << bits::isBitNull(rawNulls, 5);

    > true

We can also use helper function allocateNulls:

.. code-block:: c++

    // Allocate nulls buffer to fit 100 null flags and initialize these to bits::kNotNull.
    auto nulls = allocateNulls(100, pool.get());

    // Allocate nulls buffer to fit 100 null flags and initialize these to bits::kNull.
    auto nulls = allocateNulls(100, pool.get(), bits::kNull);

Finally, we can fill in the nulls buffer “manually”. Let’s set every other row to null.

.. code-block:: c++

    auto* rawNulls = nulls->asMutable<uint64_t>();

    for (auto i = 0; i < 10; ++i) {
     bits::setNull(rawNulls, i, i % 2 == 0);
    }

    for (auto i = 0; i < 10; ++i) {
     LOG(INFO) << i << ": " << std::boolalpha << bits::isBitNull(rawNulls, i);
    }

    > 0: true
    > 1: false
    > 2: true
    > 3: false

We can also use printNulls helper function to print the null flags:

.. code-block:: c++

    LOG(INFO) << printNulls(nulls, 10);

    > 99 out of 104 rows are null: n.n.n.n.n.

    LOG(INFO) << printNulls(nulls);

    > 99 out of 104 rows are null: n.n.n.n.n.nnnnnnnnnnnnnnnnnnnn

printNulls function takes a nulls buffer and an optional maxBitsToPrint number
which has a default value of 30.

.. code-block:: c++

    std::string printNulls(
       const BufferPtr& nulls,
       vector_size_t maxBitsToPrint = 30);

This function returns a string where each character represents a single null
flag: ‘n’ for null and ‘.’ for non-null. The result string also includes a
summary prefix telling us how many entries are null. Notice that the summary
says that there are a total of 104 entries, not 100. This is because BufferPtr
doesn’t know that it is used to store null flags. It just knows its size in
bytes, which is 13. The last byte has some bits unused.

We have learned how to allocate memory and fill it in with values and null
flags. We are now ready to assemble a flat vector to hold data for a single
column. Let’s make a vector to store 100 sequential BIGINT values with every
other value being null: [0, null, 2, null, 4, null, 6,..].

.. code-block:: c++

    #include "velox/vector/FlatVector.h"

    auto vector = std::make_shared<FlatVector<int64_t>>(
       pool.get(), BIGINT(), nulls, 100, values, std::vector<BufferPtr>{});

    LOG(INFO) << vector->toString();

    > [FLAT BIGINT: 100 elements, 50 nulls]

To make a vector, we use FlatVector<T> class template with T being int64_t
(64-bit integer). We pass a pointer to MemoryPool, a Type object that describes
the type of values to store, nulls buffer, number of values, values buffer and
an empty list of string buffers. Let’s ignore the string buffers for now. We’ll
discuss these later.

FlatValue<T> class can be used to store values of primitive types. The following
types are supported in Velox:

======================  ===========================    ==================
Type                    C++ Type                       Description
======================  ===========================    ==================
BOOLEAN                 bool                           A boolean flag: true or false.
TINYINT                 int8_t                         8-bit integer.
SMALLINT                int16_t                        16-bit integer.
INTEGER                 int32_t	                       32-bit integer.
BIGINT                  int64_t                        64-bit integer.
REAL                    float                          32-bit floating point number.
DOUBLE                  double                         64-bit floating point number.
VARCHAR                 struct StringView              Variable width string.
======================  ===========================    ==================

Nulls buffer can be null, which indicates that all values are not null.

.. code-block:: c++

    auto nonNullVector = std::make_shared<FlatVector<int64_t>>(
       pool.get(), BIGINT(), nullptr, 100, values, std::vector<BufferPtr>{});

    LOG(INFO) << nonNullVector->toString();

    > [FLAT BIGINT: 100 elements, no nulls]

Values buffer can also be null in case all values are null.

.. code-block:: c++

    auto nulls = allocateNulls(100, pool.get(), bits::kNull);
    auto allNullVector = std::make_shared<FlatVector<int64_t>>(
       pool.get(), BIGINT(), nulls, 100, nullptr, std::vector<BufferPtr>{});

    LOG(INFO) << allNullVector->toString();

    > [FLAT BIGINT: 100 elements, 100 nulls]

However, it is invalid to create a vector with both nulls and values buffers being null.

.. code-block:: c++

    std::make_shared<FlatVector<int64_t>>(
       pool.get(), BIGINT(), nullptr, 100, nullptr, std::vector<BufferPtr>{});

    VeloxRuntimeError
    Error Source: RUNTIME
    Error Code: INVALID_STATE
    Reason: FlatVector needs to either have values or nulls
    Retriable: False
    Expression: values_ || BaseVector::nulls_
    Function: FlatVector
    File: /Users/mbasmanova/cpp/velox-1/./velox/vector/FlatVector.h
    Line: 89

FlatVector::size() and FlatVector::type() getters return the number and type of
values stored in the vector.

.. code-block:: c++

    LOG(INFO) << vector->size();
    LOG(INFO) << vector->type()->toString();

    > 100
    > BIGINT

FlatVector::isNullAt(index) and FlatVector::isValueAt(index) return the null
flag and the value at specified index (row).

.. code-block:: c++

    LOG(INFO) << std::boolalpha << vector->isNullAt(5);

    > false

    LOG(INFO) << vector->valueAt(5);

    > 5

    LOG(INFO) << std::boolalpha << vector->isNullAt(6);

    > true

    LOG(INFO) << vector->valueAt(5);

    > 6

Notice that the values buffer has a value for all positions even the ones that
are null. However, the value for null positions cannot be trusted. It can be
any value.

In this chapter we have learned how to allocate memory and create vectors of
integers. In the next chapter we’ll look into how to create vectors of
strings.
