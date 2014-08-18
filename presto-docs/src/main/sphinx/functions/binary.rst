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
