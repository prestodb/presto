==================
Pinot Functions
==================

These functions are used with Pinot connector.

Type conversion
----------------

.. function:: pinot_binary_decimal_to_double(binary, bigIntegerRadix, scale, returnZeroOnNull) -> double

    Converts pinot ``binary`` decimal to double using ``bigIntegerRadix`` and ``scale``. Returns zero on null input
    if ``returnZeroOnNull`` boolean is true.