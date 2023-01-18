================
Binary Functions
================

.. function:: crc32(binary) -> bigint

    Computes the crc32 checksum of ``binary``.

.. function:: from_base64(string) -> varbinary

    Decodes binary data from the base64 encoded ``string``.

.. function:: from_hex(string) -> varbinary

    Decodes binary data from the hex encoded ``string``.

.. function:: hmac_sha1(binary, key) -> varbinary

    Computes the HMAC with sha1 of ``binary`` with the given ``key``.

.. function:: hmac_sha256(binary, key) -> varbinary

    Computes the HMAC with sha256 of ``binary`` with the given ``key``.

.. function:: hmac_sha512(binary, key) -> varbinary

    Computes the HMAC with sha512 of ``binary`` with the given ``key``.

.. function:: md5(binary) -> varbinary

    Computes the md5 hash of ``binary``.

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

.. function:: to_hex(binary) -> varchar

    Encodes ``binary`` into a hex string representation.

.. function:: xxhash64(binary) -> varbinary

    Computes the xxhash64 hash of ``binary``.
