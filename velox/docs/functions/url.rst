=============
URL Functions
=============

Encoding Functions
------------------

.. function:: url_encode(value) -> varchar

    Escapes ``value`` by encoding it so that it can be safely included in
    URL query parameter names and values:

    * Alphanumeric characters are not encoded.
    * The characters ``.``, ``-``, ``*`` and ``_`` are not encoded.
    * The ASCII space character is encoded as ``+``.
    * All other characters are converted to UTF-8 and the bytes are encoded
      as the string ``%XX`` where ``XX`` is the uppercase hexadecimal
      value of the UTF-8 byte.

.. function:: url_decode(value) -> varchar

    Unescapes the URL encoded ``value``.
    This function is the inverse of :func:`url_encode`.
