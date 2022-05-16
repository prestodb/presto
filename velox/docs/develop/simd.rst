===================
SIMD Usage in Velox
===================

SIMD uses special registers in CPU to operate on multiple primitive data
simultaneously.  In some basic cases compiler is able to translate a tight loop
into SIMD instructions for us, but often we need to call the SIMD intrinsics
explicitly.

There are several places in Velox where we use SIMD explicitly to get better
performance.  We use `xsimd`_ as a zero-cost abstraction over the intrinsics, to
address the portability issue.

.. _xsimd: https://github.com/xtensor-stack/xsimd

Architectures
-------------

In Velox we support 2 families of CPU architectures regarding SIMD: X86 and ARM.
In X86 there are 3 generations of SIMD technologies: SSE, AVX, and AVX512.  For
ARM there are NEON and SVE.  Each architecture has its own size of registers, we
summarize the details below:

============ ==================== ============= ==========
Architecture Register Size (bits) Used in Velox CPU Family
============ ==================== ============= ==========
SSE          128                  Yes           x86
AVX          256                  Yes           x86
AVX512       512                  No            x86
NEON         128                  Yes           ARM
SVE          128 - 2048           No            ARM
============ ==================== ============= ==========

xsimd Basics
------------

The data structure in ``xsimd`` to represent a SIMD register is ``batch<T, A>``.
``T`` stands for the element data type and ``A`` stands for the architecture.
For example ``batch<int32_t, avx2>`` represents a SIMD vector containing 32 bits
signed integers on AVX2.  This type has only 1 field called ``data``, which is
the underlying SIMD register (e.g. for AVX it can be ``__m256``, ``__m256d``, or
``__m256i``).  This ensures the data structure can be optimized directly as the
register without any overhead during runtime.

When you compare 2 SIMD vectors (e.g. ``x == y``), there are 2 kinds of result
type, depending on the architecture.  For AVX512, the comparison result is kept
as a bit mask (1 bit per element, up to 64 bits) in a normal integer.  For all
other architectures, the result is kept in another SIMD register with the same
number of lanes as the operands, and each element in the result is set to
either -1 (``true``) or 0 (``false``).  In ``xsimd`` these 2 types are unified
to one type: ``batch_bool<T, A>``.  ``T`` is the element type of comparison
operands, and ``A`` is the architecture.

``xsimd`` provides some functions and operators to abstract the intrinsics on
different architectures, including basic arithmetics, comparisons, bitwise
operations, mathematical functions, loading or storing from memory.

SIMD Utilities
--------------

There are some intrinsics that are not yet abstracted by ``xsimd``.  We added
the ones commonly used in Velox in ``common/base/SimdUtil.h``.

HalfBatch
~~~~~~~~~

In ``xsimd`` the vector size is decided uniquely by the architecture ``A``.  In
some cases we need a different size of vector though, for example in gather, if
the data type is 64 bits and index type is 32 bits, the vector for indices needs
to be the half size of the vector for data.  To accommodate such needs, we
define a type ``HalfBatch<T, A>`` to get the corresponding vector type.

In some cases when the default vector size is 128 bits, there is no
corresponding SIMD vector of 64 bits to be used as ``HalfBatch``.  In such cases
we define and use a type ``Batch64<T>``, with some methods and operators same as
``batch<T, A>``, so that we can use them interchangeably.

Gather
~~~~~~

Gather is an operation to load a vector from non-contiguous memory.  In the
simplest form, given a ``base`` address and a list of ``indices`` (saved in a
SIMD vector), gather returns another vector containing all elements at

::

   base + indices[0]
   base + indices[1]
   ...
   base + indices[n]

A variance of gather called ``maskGather`` takes an extra vector ``src`` and a
``batch_bool`` mask, only loads the data from corresponding memory address if
``mask[i]`` is set, otherwise uses the element in ``src[i]``.  In other words,
the function returns ``dst`` where

::

   if mask[i]
     dst[i] = load(base + indices[i])
   else
     dst[i] = src[i]

Bit Masks
~~~~~~~~~

As mentioned above, ``batch_bool`` is used to represent the result of a
comparison, and the underlying data can be either a bit mask or a SIMD vector.
To allow us manipulate this result, we provide some utilities to convert between
``batch_bool`` and bit mask (``toBitMask`` and ``fromBitMask``).  Once you
convert it to bit mask, you can use the normal bit manipulating operations on
it.  We also provide utilities like ``leadingMask`` and ``allSetBitMask`` to
make it easier and faster to manipulate bits.

Filter
~~~~~~

Another important function we have in ``SimdUtil.h`` is ``filter``.  It takes a
SIMD vector ``data`` and a ``bitMask``, then for each ``i`` where ``bitMask[i]``
is set, we move the corresponding ``data[i]`` to front and return the result.
This behaves very similar to ``std::partition``.  In other words, the function
returns ``dst`` where

::

   j = 0
   for i in 0 to n
     if bitMask[i]
       dst[j++] = data[i]
   for i in 0 to n
     if not bitMask[i]
       dst[j++] = data[i]

BMI Utilities
-------------

In addition to SIMD abstraction and utilities, we also have some functions that
depend on BMI2 intrinsics.  We define the portable version of them in
``common/base/BitUtil.h``.  These functions include ``extractBits`` and
``rotateLeft``.  They are relatively simple and standalone comparing to SIMD,
and you can refer the documentation in the file for their usage.

Use Cases
---------

Hash Table
~~~~~~~~~~

In ``BigintValuesUsingHashTable::testValues`` we use SIMD to check whether
multiple values are in the hash table at same time.  In the hash table we use a
special empty marker to indicate the value is missing.  The process is
following:

1. If all values are out of range, we can return all false.
2. If empty marker has been inserted into the hash table, fall back to check the
   values one by one.
3. Hash all valid values using SIMD multiplication and modulo, and then get the
   corresponding states in hash table using ``maskGather``.
4. If the state is empty marker, the value is missing; if the state is equal to
   value, the value is found.  Otherwise we have an hash collision and need to
   look at next positions in hash table.  If no collision is happening, we can
   return the result right away.
5. For each value that has collision, we use SIMD to advance multiple positions
   at once, until we find either value match or empty mark.

Filtering
~~~~~~~~~

A typical use case for filtering values using SIMD is in ``processFixedFilter``
from ``dwio/dwrf/common/DecoderUtil.h``.  This function evaluates the filter on
a batch of values, and stores the passed row numbers from this batch to
``filterHits``, and the passed values to ``rawValues``.

The filtering on values is done using ``Filter::testValues``, the result is
stored in a bit mask.  We then pass the bit mask to ``simd::filter`` to store
indices and values.  Finally we increase ``numValues`` with the popcount of bit
mask.

Note when the data type is 16 bits long, we need to do the process in 2 batches
(``loadIndices(0)`` and ``loadIndices(1)``), because the indices are 32 bits
long and one SIME vector is not large enough to contain all the indices needed.
