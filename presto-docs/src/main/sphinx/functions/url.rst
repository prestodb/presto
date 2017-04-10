=============
URL Functions
=============

Extraction Functions
--------------------

The URL extraction functions extract components from HTTP URLs
(or any valid URIs conforming to :rfc:`2396`).
The following syntax is supported:

.. code-block:: none

    [protocol:][//host[:port]][path][?query][#fragment]

The extracted components do not contain URI syntax separators
such as ``:`` or ``?``.

.. function:: url_extract_fragment(url) -> varchar

    Returns the fragment identifier from ``url``.

.. function:: url_extract_host(url) -> varchar

    Returns the host from ``url``.

.. function:: url_extract_parameter(url, name) -> varchar

    Returns the value of the first query string parameter named ``name``
    from ``url``. Parameter extraction is handled in the typical manner
    as specified by :rfc:`1866#section-8.2.1`.

.. function:: url_extract_path(url) -> varchar

    Returns the path from ``url``.

.. function:: url_extract_port(url) -> bigint

    Returns the port number from ``url``.

.. function:: url_extract_protocol(url) -> varchar

    Returns the protocol from ``url``.

.. function:: url_extract_query(url) -> varchar

    Returns the query string from ``url``.

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
