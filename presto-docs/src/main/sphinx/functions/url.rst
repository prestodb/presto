=============
URL Functions
=============

The URL functions extract components from HTTP URLs
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
