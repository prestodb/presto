================
Window functions
================

Spark window functions can be used to compute SQL window functions.

Value functions
---------------

.. function:: nth_value(x, offset) -> [same as input]
   :noindex:

Returns the value at the specified offset from the beginning of the window. Offsets start at 1.
The offset should be a positive int literal. If the offset is greater than the number of values
in the window, null is returned. It is an error for the offset to be zero or negative.

Rank functions
---------------

.. spark:function:: row_number() -> integer

Returns a unique, sequential number to each row, starting with one, according to the ordering of rows within the window partition.