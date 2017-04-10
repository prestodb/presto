================
Binary Functions
================

Binary Functions
----------------

.. function:: length(binary) -> bigint
    :noindex:

    Returns the length of ``binary`` in bytes.

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
