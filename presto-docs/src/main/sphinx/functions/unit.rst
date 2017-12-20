==============
Unit Functions
==============

Data Size Functions
-------------------

The data size functions support the following units:

======= =============
Unit    Description
======= =============
``B``   Bytes
``kB``  Kilo bytes
``MB``  Mega bytes
``GB``  Giga bytes
``TB``  Tera bytes
``PB``  Peta bytes
======= =============

.. function:: succinct_bytes(bytes) -> varchar

    Returns a succinct representation of the byte size::

        SELECT succinct_bytes(1024 * 1024); -- '1.00MB'

.. function:: succinct_data_size(value, unit) -> varchar

    Returns a succinct representation of the data size. ``unit`` is one of the data size units in the above::

        SELECT succinct_data_size(2048, 'MB') -- '2.00GB'

Duration Functions
------------------

See :ref:`duration_functions`.
