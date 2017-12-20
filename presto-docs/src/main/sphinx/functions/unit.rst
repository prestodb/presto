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
``kB``  Kilobyte
``MB``  Megabyte
``GB``  Gigabyte
``TB``  Terabyte
``PB``  Petabyte
======= =============

.. function:: succinct_bytes(bytes) -> varchar

    Returns a succinct representation of ``bytes``.::

        SELECT succinct_bytes(1024 * 1024); -- '1.00MB'

.. function:: succinct_data_size(value, unit) -> varchar

    Returns a succinct representation of the data size specified by ``value`` and ``unit``.
    ``unit`` must be one of the supported data size units::

        SELECT succinct_data_size(2048, 'MB') -- '2.00GB'

Duration Functions
------------------

See :ref:`duration_functions`.
