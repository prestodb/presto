================
Binary Functions
================

Binary Functions
----------------

.. function:: length(varbinary) -> bigint
    :noindex:

    Returns the length of ``varbinary`` in bytes.

.. function:: to_base64(varbinary) -> varchar

    Encodes ``varbinary`` into a base64 string representation.

.. function:: from_base64(varchar) -> varbinary

    Decodes binary data from the base64 encoded ``varchar``.

.. function:: to_base64url(varbinary) -> varchar

    Encodes ``varbinary`` into a base64 string representation using the URL safe alphabet.

.. function:: from_base64url(varchar) -> varbinary

    Decodes binary data from the base64 encoded ``varchar`` using the URL safe alphabet.

.. function:: to_hex(varbinary) -> varchar

    Encodes ``varbinary`` into a hex string representation.

.. function:: from_hex(varchar) -> varbinary

    Decodes binary data from the hex encoded ``varchar``.

.. function:: md5(varbinary) -> varbinary

    Computes the md5 hash of ``varbinary``.
