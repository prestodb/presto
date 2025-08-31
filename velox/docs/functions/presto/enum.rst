==============
Enum Functions
==============

.. function:: enum_key(x) -> varchar

    Returns the string key of the enum value, where ``x`` is either a BigintEnum or VarcharEnum value. ::

        SELECT enum_key(alpha.A); -- "A"

    where alpha is a BigintEnum type with name "alpha" and values {"A": 1, "B": 2}
