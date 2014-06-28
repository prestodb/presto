====================================
数学函数和运算
====================================

数学运算
----------------------

======== ===========
运算符    描述
======== ===========
``+``    加法
``-``    减法
``*``    乘法
``/``    除法 (整数除法将被截断)
``%``    取模 (余数)
======== ===========

数学函数
----------------------

.. function:: abs(x) -> [和输入一样]

    返回 ``x`` 的绝对值。

.. function:: cbrt(x) -> double

    返回 ``x`` 的立方根。

.. function:: ceil(x) -> [和输入一样]

    :func:`ceiling` 的别名。

.. function:: ceiling(x) -> [和输入一样]

    返回 ``x`` 的上取整。

.. function:: e() -> double

    返回自然底数e的值。

.. function:: exp(x) -> double

    返回自然底数e的 ``x`` 次方。

.. function:: floor(x) -> [和输入一样]

    返回 ``x`` 的下取整。

.. function:: ln(x) -> double

    返回 ``x`` 的自然对数。

.. function:: log2(x) -> double

    返回以2为底 ``x`` 的对数。

.. function:: log10(x) -> double

    返回以10为底 ``x`` 的对数。

.. function:: log(x, b) -> double

    返回以 ``b`` 为底 ``x`` 的对数。

.. function:: mod(n, m) -> [和输入一样]

    返回 ``n`` 除以 ``m`` 的模(余数)。

.. function:: pi() -> double

    返回常量π的值。

.. function:: pow(x, p) -> double

    返回 ``x`` 的 ``p`` 次方。

.. function:: rand() -> double

    ``random()`` 函数的别名。

.. function:: random() -> double

    返回一个伪随机值x位于 ``0.0 <= x < 1.0`` 之间。

.. function:: round(x) -> [和输入一样]

    返回 ``x`` 的近似整数。

.. function:: round(x, d) -> [和输入一样]

    Returns ``x`` rounded to ``d`` decimal places.

.. function:: sqrt(x) -> double

    返回 ``x`` 的平方根。

三角函数
-----------------------

所有的三角函数的参数都用弧度表示。

.. function:: acos(x) -> double

    返回 ``x`` 的反余弦值。

.. function:: asin(x) -> double

    返回 ``x`` 的反正弦值。

.. function:: atan(x) -> double

    返回 ``x`` 的反正切值。

.. function:: atan2(y, x) -> double

    返回 ``y / x`` 的反正切值。

.. function:: cos(x) -> double

    返回 ``x`` 的余弦值。

.. function:: cosh(x) -> double

    返回 ``x`` 的双曲余弦值。

.. function:: sin(x) -> double

    返回 ``x`` 的正弦值。

.. function:: tan(x) -> double

    返回 ``x`` 的正切值。

.. function:: tanh(x) -> double

    返回 ``x`` 的双曲正切值。

浮点函数
------------------------

.. function:: infinity() -> double

    返回正无穷。

.. function:: is_finite(x) -> boolean

    判断 ``x`` 是否为有限值。

.. function:: is_infinite(x) -> boolean

    判断 ``x`` 是否为正穷值。

.. function:: is_nan(x) -> boolean

    判断 ``x`` 是否为NAN。

.. function:: nan() -> double

    返回NAN。
