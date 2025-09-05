*****************
Iceberg Functions
*****************

Here is a list of all scalar Iceberg functions available in Velox.
Function names link to function description.

These functions are used in partition transform.
Refer to `Iceberg documenation <https://iceberg.apache.org/spec/#partition-transforms>`_ for details.

.. iceberg:function:: bucket(numBuckets, input) -> integer

   Returns an integer between 0 and numBuckets - 1, indicating the assigned bucket.
   Bucket partitioning is based on a 32-bit hash of the input, specifically using the x86
   variant of the Murmur3 hash function with a seed of 0.

   The function can be expressed in pseudo-code as below. ::

       def bucket(numBuckets, input)= (murmur3_x86_32_hash(input) & Integer.MAX_VALUE) % numBuckets

   The ``numBuckets`` is of type INTEGER and must be greater than 0. Otherwise, an exception is thrown.
   Supported types for ``input`` are INTEGER, BIGINT, DECIMAL, DATE, TIMESTAMP, VARCHAR, VARBINARY. ::
       SELECT bucket(128, 'abcd'); -- 4
       SELECT bucket(100, 34L); -- 79
