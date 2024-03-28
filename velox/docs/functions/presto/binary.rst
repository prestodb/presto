================
Binary Functions
================

.. function:: crc32(binary) -> bigint

    Computes the crc32 checksum of ``binary``.

.. function:: from_base64(string) -> varbinary

    Decodes binary data from the base64 encoded ``string``.

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

.. function:: md5(binary) -> varbinary

    Computes the md5 hash of ``binary``.

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
