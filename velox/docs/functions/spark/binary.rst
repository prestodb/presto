================
Binary Functions
================

.. spark:function:: hash(x) -> integer

    Computes the hash of x.

.. spark:function:: md5(x) -> varbinary

    Computes the md5 of x.

.. spark:function:: sha1(x) -> varchar

    Computes SHA-1 digest of x and convert the result to a hex string.
    Note: x can only be varbinary type.

.. spark:function:: xxhash64(x) -> integer

    Computes the xxhash64 of x.
