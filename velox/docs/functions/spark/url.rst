=============
URL Functions
=============

Introduction
------------

The URL extraction functions extract components from HTTP URLs (or any valid URIs conforming to `RFC 3986 <https://tools.ietf.org/html/rfc3986.html>`_). The following syntax is supported:

.. code-block:: bash

    [protocol:][//host[:port]][path][?query][#fragment]


The extracted components do not contain URI syntax separators such as ``:`` , ``?`` and ``#``.

Consider for example the below URI:

.. code-block::

    http://www.ics.uci.edu/pub/ietf/uri/?k1=v1#Related

    scheme    = http
    authority = www.ics.uci.edu
    path      = /pub/ietf/uri/
    query     = k1=v1
    fragment  = Related


Invalid URI's
-------------

Well formed URI's should not contain ascii whitespace. `Percent-encoded URI's <https://www.rfc-editor.org/rfc/rfc3986#section-2.1>`_ should be followed by two hexadecimal
digits after the percent character "%". All the url extract functions will return null when passed an invalid uri.

.. code-block::

    # Examples of url functions with Invalid URI's.

    # Invalid URI due to whitespace
    SELECT url_extract_path('foo '); -- NULL (1 row)
    SELECT url_extract_host('http://www.foo.com '); -- NULL (1 row)

    # Invalid URI due to improper escaping of '%'
    SELECT url_extract_path('https://www.ucu.edu.uy/agenda/evento/%%UCUrlCompartir%%'); -- NULL (1 row)
    SELECT url_extract_host('https://www.ucu.edu.uy/agenda/evento/%%UCUrlCompartir%%'); -- NULL (1 row)

Encoding Functions
------------------

.. spark::function:: url_encode(value) -> varchar

    Escapes ``value`` by encoding it so that it can be safely included in
    URL query parameter names and values:

    * Alphanumeric characters are not encoded.
    * The characters ``.``, ``-``, ``*`` and ``_`` are not encoded.
    * The ASCII space character is encoded as ``+``.
    * All other characters are converted to UTF-8 and the bytes are encoded
      as the string ``%XX`` where ``XX`` is the uppercase hexadecimal
      value of the UTF-8 byte.

.. spark::function:: url_decode(value) -> varchar

    Unescapes the URL encoded ``value``.
    This function is the inverse of :func:`url_encode`.
