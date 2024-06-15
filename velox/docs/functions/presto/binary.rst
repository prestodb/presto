================
Binary Functions
================

.. function:: crc32(binary) -> bigint

    Computes the crc32 checksum of ``binary``.

.. function:: from_base64(string) -> varbinary

    Decodes a Base64-encoded ``string`` back into its original binary form. 
    This function is capable of handling both fully padded and non-padded Base64 encoded strings. 
    Partially padded Base64 strings are not supported and will result in an error.

    Examples
    --------
    Query with padded Base64 string:
    ::
        SELECT from_base64('SGVsbG8gV29ybGQ='); -- [72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100]

    Query with non-padded Base64 string:
    ::
        SELECT from_base64('SGVsbG8gV29ybGQ'); -- [72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100]

    Query with partial-padded Base64 string:
    ::
        SELECT from_base64('SGVsbG8gV29ybGQgZm9yIHZlbG94IQ='); -- Error : Base64::decode() - invalid input string: string length is not a multiple of 4.

    In the above examples, both the fully padded and non-padded Base64 strings ('SGVsbG8gV29ybGQ=' and 'SGVsbG8gV29ybGQ') decode to the binary representation of the text 'Hello World'.
    While, partial-padded Base64 string 'SGVsbG8gV29ybGQgZm9yIHZlbG94IQ=' will lead to an velox error.

.. function:: from_base64url(string) -> varbinary

    Decodes ``string`` data from the base64 encoded representation using the `URL safe alphabet <https://www.rfc-editor.org/rfc/rfc4648#section-5>`_ into a varbinary.

.. function:: from_big_endian_32(varbinary) -> integer

    Decodes ``integer`` value from a 32-bit 2’s complement big endian ``binary``.

.. function:: from_big_endian_64(varbinary) -> bigint

    Decodes ``bigint`` value from a 64-bit 2’s complement big endian ``binary``.

.. function:: from_hex(string) -> varbinary

    Decodes binary data from the hex encoded ``string``.

.. function:: from_ieee754_32(binary) -> real

    Decodes the 32-bit big-endian ``binary`` in IEEE 754 single-precision floating-point format.
    Throws a user error if input size is shorter / longer than 32 bits.

.. function:: from_ieee754_64(binary) -> double

    Decodes the 64-bit big-endian ``binary`` in IEEE 754 double-precision floating-point format.
    Throws a user error if input size is shorter / longer than 64 bits.

.. function:: hmac_md5(binary, key) -> varbinary

    Computes the HMAC with md5 of ``binary`` with the given ``key``.

.. function:: hmac_sha1(binary, key) -> varbinary

    Computes the HMAC with sha1 of ``binary`` with the given ``key``.

.. function:: hmac_sha256(binary, key) -> varbinary

    Computes the HMAC with sha256 of ``binary`` with the given ``key``.

.. function:: hmac_sha512(binary, key) -> varbinary

    Computes the HMAC with sha512 of ``binary`` with the given ``key``.

.. function:: length(binary) -> bigint

    Returns the length of ``binary`` in bytes.

.. function:: lpad(binary, size, padbinary) -> varbinary
    :noindex:
    
    Left pads ``binary`` to ``size`` bytes with ``padbinary``.
    If ``size`` is less than the length of ``binary``, the result is
    truncated to ``size`` characters. ``size`` must not be negative
    and ``padbinary`` must be non-empty. ``size`` has a maximum value of 1 MiB.
    In the case of ``size`` being smaller than the length of ``binary``, 
    ``binary`` will be truncated from the right to fit the ``size``.

.. function:: md5(binary) -> varbinary

    Computes the md5 hash of ``binary``.

.. function:: rpad(binary, size, padbinary) -> varbinary
    :noindex:

    Right pads ``binary`` to ``size`` bytes with ``padbinary``.
    If ``size`` is less than the length of ``binary``, the result is
    truncated to ``size`` characters. ``size`` must not be negative
    and ``padbinary`` must be non-empty. ``size`` has a maximum value of 1 MiB.
    In the case of ``size`` being smaller than the length of ``binary``, 
    ``binary`` will be truncated from the right to fit the ``size``.
    
.. function:: sha1(binary) -> varbinary

    Computes the SHA-1 hash of ``binary``.

.. function:: sha256(binary) -> varbinary

    Computes the SHA-256 hash of ``binary``.

.. function:: sha512(binary) -> varbinary

    Computes the SHA-512 hash of ``binary``.

.. function:: spooky_hash_v2_32(binary) -> varbinary

    Computes the SpookyHashV2 32-bit hash of ``binary``.

.. function:: spooky_hash_v2_64(binary) -> varbinary

    Computes the 64-bit SpookyHashV2 hash of ``binary``.

.. function:: to_base64(binary) -> varchar

    Encodes ``binary`` into a base64 string representation.

.. function:: to_base64url(binary) -> varchar

    Encodes ``binary`` into a base64 string representation using the `URL safe alphabet <https://www.rfc-editor.org/rfc/rfc4648#section-5>`_.

 .. function:: to_big_endian_32(integer) -> varbinary

     Encodes ``integer`` in a 32-bit 2’s complement big endian format.

 .. function:: to_big_endian_64(bigint) -> varbinary

     Encodes ``bigint`` in a 64-bit 2’s complement big endian format.

.. function:: to_hex(binary) -> varchar

    Encodes ``binary`` into a hex string representation.

.. function:: to_ieee754_32(real) -> varbinary

    Encodes ``real`` in a 32-bit big-endian binary according to IEEE 754 single-precision floating-point format.

.. function:: to_ieee754_64(double) -> varbinary

    Encodes ``double`` in a 64-bit big-endian binary according to IEEE 754 double-precision floating-point format.

.. function:: xxhash64(binary) -> varbinary

    Computes the xxhash64 hash of ``binary``.
