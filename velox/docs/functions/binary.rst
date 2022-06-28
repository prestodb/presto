================
Binary Functions
================

.. function:: xxhash64(binary) -> varbinary

    Computes the xxhash64 hash of ``binary``.

.. function:: md5(binary) -> varbinary

    Computes the md5 hash of ``binary``.

.. function:: sha256(binary) -> varbinary

    Computes the SHA-256 hash of ``binary``.

.. function:: sha512(binary) -> varbinary

    Computes the SHA-512 hash of ``binary``.

.. function:: to_base64(binary) -> varchar

    Encodes ``binary`` into a base64 string representation.

.. function:: from_base64(string) -> varbinary

    Decodes binary data from the base64 encoded ``string``.

.. function:: to_hex(binary) -> varchar

    Encodes ``binary`` into a hex string representation.

.. function:: from_hex(string) -> varbinary

    Decodes binary data from the hex encoded ``string``.
