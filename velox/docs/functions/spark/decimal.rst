===================================
Decimal functions and special forms
===================================

Decimal Functions
-----------------

.. spark:function:: unscaled_value(x) -> bigint

    Return the unscaled bigint value of a short decimal ``x``.
    Supported type is: SHORT_DECIMAL.

Decimal Special Forms
---------------------

.. spark:function:: make_decimal(x[, nullOnOverflow]) -> decimal

    Create ``decimal`` of requsted precision and scale from an unscaled bigint value ``x``.
    By default, the value of ``nullOnOverflow`` is true, and null will be returned when ``x`` is too large for the result precision.
    Otherwise, exception will be thrown when ``x`` overflows.
