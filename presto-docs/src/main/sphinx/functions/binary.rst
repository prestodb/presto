==============================
Binary Functions and Operators
==============================

Binary Operators
----------------

The ``||`` operator performs concatenation.

Binary Functions
----------------

.. function:: length(binary) -> bigint
    :noindex:

    Returns the length of ``binary`` in bytes.

.. function:: concat(binary1, ..., binaryN) -> varbinary
    :noindex:

    Returns the concatenation of ``binary1``, ``binary2``, ``...``, ``binaryN``.
    This function provides the same functionality as the
    SQL-standard concatenation operator (``||``).

.. function:: substr(binary, start) -> varbinary
    :noindex:

    Returns the rest of ``binary`` from the starting position ``start``,
    measured in bytes. Positions start with ``1``. A negative starting position
    is interpreted as being relative to the end of the string.

.. function:: substr(binary, start, length) -> varbinary
    :noindex:

    Returns a substring from ``binary`` of length ``length`` from the starting
    position ``start``, measured in bytes. Positions start with ``1``. A
    negative starting position is interpreted as being relative to the end of
    the string.

.. function:: to_base64(binary) -> varchar

    Encodes ``binary`` into a base64 string representation.

.. function:: from_base64(string) -> varbinary

    Decodes binary data from the base64 encoded ``string``.

.. function:: to_base64url(binary) -> varchar

    Encodes ``binary`` into a base64 string representation using the URL safe alphabet.

.. function:: from_base64url(string) -> varbinary

    Decodes binary data from the base64 encoded ``string`` using the URL safe alphabet.

.. function:: to_hex(binary) -> varchar

    Encodes ``binary`` into a hex string representation.

.. function:: from_hex(string) -> varbinary

    Decodes binary data from the hex encoded ``string``.

.. function:: to_big_endian_64(bigint) -> varbinary

    Encodes ``bigint`` in a 64-bit 2's complement big endian format.

.. function:: from_big_endian_64(binary) -> bigint

    Decodes ``bigint`` value from a 64-bit 2's complement big endian ``binary``.

.. function:: to_big_endian_32(integer) -> varbinary

    Encodes ``integer`` in a 32-bit 2's complement big endian format.

.. function:: from_big_endian_32(binary) -> integer

    Decodes ``integer`` value from a 32-bit 2's complement big endian ``binary``.

.. function:: to_ieee754_32(real) -> varbinary

    Encodes ``real`` in a 32-bit big-endian binary according to IEEE 754 single-precision floating-point format.

.. function:: from_ieee754_32(binary) -> real

    Decodes the 32-bit big-endian ``binary`` in IEEE 754 single-precision floating-point format.

.. function:: to_ieee754_64(double) -> varbinary

    Encodes ``double`` in a 64-bit big-endian binary according to IEEE 754 double-precision floating-point format.

.. function:: from_ieee754_64(binary) -> double

    Decodes the 64-bit big-endian ``binary`` in IEEE 754 double-precision floating-point format.

.. function:: lpad(binary, size, padbinary) -> varbinary
    :noindex:

    Left pads ``binary`` to ``size`` bytes with ``padbinary``.
    If ``size`` is less than the length of ``binary``, the result is
    truncated to ``size`` characters. ``size`` must not be negative
    and ``padbinary`` must be non-empty.

.. function:: rpad(binary, size, padbinary) -> varbinary
    :noindex:

    Right pads ``binary`` to ``size`` bytes with ``padbinary``.
    If ``size`` is less than the length of ``binary``, the result is
    truncated to ``size`` characters. ``size`` must not be negative
    and ``padbinary`` must be non-empty.

.. function:: crc32(binary) -> bigint

    Computes the CRC-32 of ``binary``. For general purpose hashing, use
    :func:`xxhash64`, as it is much faster and produces a better quality hash.

.. function:: md5(binary) -> varbinary

    Computes the md5 hash of ``binary``.

.. function:: sha1(binary) -> varbinary

    Computes the sha1 hash of ``binary``.

.. function:: sha256(binary) -> varbinary

    Computes the sha256 hash of ``binary``.

.. function:: sha512(binary) -> varbinary

    Computes the sha512 hash of ``binary``.

.. function:: xxhash64(binary) -> varbinary

    Computes the xxhash64 hash of ``binary``.

.. function:: spooky_hash_v2_32(binary) -> varbinary

    Computes the 32-bit SpookyHashV2 hash of ``binary``.

.. function:: spooky_hash_v2_64(binary) -> varbinary

    Computes the 64-bit SpookyHashV2 hash of ``binary``.

.. function:: hmac_md5(binary, key) -> varbinary

    Computes HMAC with md5 of ``binary`` with the given ``key``.

.. function:: hmac_sha1(binary, key) -> varbinary

    Computes HMAC with sha1 of ``binary`` with the given ``key``.

.. function:: hmac_sha256(binary, key) -> varbinary

    Computes HMAC with sha256 of ``binary`` with the given ``key``.

.. function:: hmac_sha512(binary, key) -> varbinary

    Computes HMAC with sha512 of ``binary`` with the given ``key``.
