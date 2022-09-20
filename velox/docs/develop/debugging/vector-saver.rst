==============================================
VectorSaver: Encoding-Preserving Serialization
==============================================

When an error occurs during expression evaluation, it is useful to save the
input vector as well as the expression to enable reproducing and debugging the
error in isolation. Saving the input vector can also be useful when an error
occurs in an operator. When saving vectors it is important to preserve the
encodings, as often a bug can be reproduced only with a specific combination of
dictionary or constant wrappers. Hence, we cannot reuse the Presto’s
SerializedPage format and need to create a different serialization format.

In this document we describe a binary serialization format that preserves the
encoding. The implementation of this format can be found in VectorSaver.h/cpp.
The examples of usage can be found in VectorSaverTest.cpp. This functionlity
was introduced in :pr:`2563`

Before we proceed to describe the format, we’d like to show an example use case.

Use Case
--------

When an error happens during expression evaluation we save the input vector to a
file and include the file path in the Context field of the exception along with
the expression that was evaluated. The exception may look like this:

.. code-block::

  Error Source: RUNTIME
  Error Code: INVALID_STATE
  Reason: (8 vs. 12) Malformed dictionary, index array is shorter than DictionaryVector
  Retriable: False
  Expression: dictionaryIndices->size() >= length * sizeof(vector_size_t)
  Context: concat(cast((c0) as VARCHAR), ,:VARCHAR). Input: /tmp/velox_vector_f7dneH.
  Function: DictionaryVector
  File: …/velox/vector/DictionaryVector-inl.h
  Line: 107

**Input: /tmp/velox_vector_f7dneH** in the `Context:` field shows the file path that
contains the input data for the expression. We can load this file into a vector in a
unit test or standalone program:

.. code-block:: c++

  #include <fstream>
  #include "velox/vector/VectorSaver.h"

  std::ifstream inputFile("/tmp/velox_vector_f7dneH", std::ifstream::binary);
  auto data = restoreVector(inputFile, pool());
  inputFile.close();

  std::cout << data->toString() << std::endl;
  std::cout << data->toString(0, 5) << std::endl;

**concat(cast((c0) as VARCHAR), ,:VARCHAR)** shows the expression. We can
evaluate it on the input vector (with minimal tweaking to replace
`,:VARCHAR` with `‘,’`) to reproduce and debug the error:

.. code-block:: c++

  auto result = evaluate(
      "concat(cast((c0) as VARCHAR), ',')",
      std::dynamic_pointer_cast<RowVector>(data));

Serialization Format
--------------------

Serialization of flat, constant and dictionary vectors is supported.
Serialization of lazy, bias and sequence vectors is not supported. Most types
are supported. Decimal, opaque and function types are not supported.

Header
~~~~~~

Vector serialization starts with a header:

* Encoding. 4 bytes
    * 0 - FLAT, 1 - CONSTANT, 2 - DICTIONARY
* Type. Variable number of bytes.
* Size. 4 bytes.

Type
~~~~

Scalar types are fully defined by the TypeKind enum. These are serialized into 4
bytes that contain the integer value of the TypeKind.

Complex types are serialized recursively. Array type is serialized into 4 bytes
for TypeKind, followed by serialization of the element’s type. Map type is
serialized into 4 bytes for TypeKind, followed by serialization of the key’s
type, followed by serialization of the value’s type. Row type is serialized
into 4 bytes for TypeKind, followed by 4 bytes for the number of children,
followed by 1st child name, 1st child type, 2nd child name, 2nd child type,
etc. Serialization of a child name starts with 4 bytes for name length,
followed by that many bytes of the name itself.

Serialization of decimal, opaque and function types is not supported.

Buffer
~~~~~~

Serialization of nulls, values, strings, indices, offsets and sizes buffers
starts with 4 bytes for buffer size, followed by that many bytes of the buffer
content.

Flat Vector of Scalar Type
~~~~~~~~~~~~~~~~~~~~~~~~~~

* Header
* Boolean indicating the presence of the nulls buffer. 1 byte.
* Nulls buffer (if present).
* Boolean indicating the presence of the values buffer. 1 byte.
* Values buffer (if present)
* Number of string buffers. 4-bytes.
* String buffers.

StringView values are serialized using a
`pointer swizzling <https://en.wikipedia.org/wiki/Pointer_swizzling>`_-like
mechanism.

Inlined string views are serialized as is.

To serialize a non-inlined string, we compute an offset within a contiguous
piece of memory formed by arranging string buffers one after the other in the
same order as stored in the stringBuffers vector. Then, serialize the string
view as 4 bytes for size, 4 bytes of zeros, 8 bytes for offset.

Both inlined and non-inlined string views serialize into 16 bytes each.

Flat Row Vector
~~~~~~~~~~~~~~~

* Header
* Boolean indicating the presence of the nulls buffer. 1 byte.
* Nulls buffer (if present).
* Number of children. 4 bytes.
* Child vectors. Each vector is preceded by a boolean indicating whether the vector is null.

Flat Array Vector
~~~~~~~~~~~~~~~~~

* Header
* Boolean indicating the presence of the nulls buffer. 1 byte.
* Nulls buffer (if present).
* Sizes buffer.
* Offsets buffer.
* Elements vector.

Flat Map Vector
~~~~~~~~~~~~~~~

* Header
* Boolean indicating the presence of the nulls buffer. 1 byte.
* Nulls buffer (if present).
* Keys vector.
* Values vector.

Constant Vector
~~~~~~~~~~~~~~~

* Header
* Is-null flag. 1 byte.
* Is-scalar-value boolean. 1 byte.
* If scalar type:
    * Scalar value.
    * If value is a non-inlined string, 4 bytes for the string size, followed by the string itself.
* If complex type:
    * Base vector
    * Index into base vector. 4 bytes.

Dictionary Vector
~~~~~~~~~~~~~~~~~

* Header
* Boolean indicating the presence of the nulls buffer. 1 byte.
* Nulls buffer (if present).
* Indices buffer.
* Base vector.
