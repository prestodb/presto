===============
Color Functions
===============

.. function:: bar(x, width) -> varchar

    Renders a single bar in an ANSI bar chart using a default
    ``low_color`` of red and a ``high_color`` of green.  For example,
    if ``x`` of 25% and width of 40 are passed to this function. A
    10-character red bar will be drawn followed by 30 spaces to create
    a bar of 40 characters.

.. function:: bar(x, width, low_color, high_color) -> varchar

    Renders a single line in an ANSI bar chart of the specified
    ``width``. The parameter ``x`` is a double value between [0,1].
    Values of ``x`` that fall outside the range [0,1] will be
    truncated to either a 0 or a 1 value. The ``low_color`` and
    ``high_color`` capture the color to use for either end of
    the horizontal bar chart.  For example, if ``x`` is 0.5, ``width``
    is 80, ``low_color`` is 0xFF0000, and ``high_color`` is 0x00FF00
    this function will return a 40 character bar that varies from red
    (0xFF0000) and yellow (0xFFFF00) and the remainder of the 80
    character bar will be padded with spaces.

    .. figure:: ../images/functions_color_bar.png
       :align: center

.. function:: color(string) -> color

    Returns a color capturing a decoded RGB value from a 4-character
    string of the format "#000".  The input string should be varchar
    containing a CSS-style short rgb string or one of ``black``,
    ``red``, ``green``, ``yellow``, ``blue``, ``magenta``, ``cyan``,
    ``white``.

.. function:: color(x, low, high, low_color, high_color) -> color

    Returns a color interpolated between ``low_color`` and
    ``high_color`` using the double parameters ``x``, ``low``, and
    ``high`` to calculate a fraction which is then passed to the
    ``color(fraction, low_color, high_color)`` function shown below.
    If ``x`` falls outside the range defined by ``low`` and ``high``
    it's value will be truncated to fit within this range.

.. function:: color(x, low_color, high_color) -> color

    Returns a color interpolated between ``low_color`` and
    ``high_color`` according to the double argument ``x`` between 0
    and 1.0.  The parameter ``x`` is a double value between [0,1].
    Values of ``x`` that fall outside the range [0,1] will be
    truncated to either a 0 or a 1 value.

.. function:: render(x, color) -> varchar

    Renders value ``x`` using the specific color using ANSI
    color codes. ``x`` can be either a double, bigint, or varchar.

.. function:: render(b) -> varchar

    Accepts boolean value ``b`` and renders a green true or a red
    false using ANSI color codes.

.. function:: rgb(red, green, blue) -> color

    Returns a color value capturing the RGB value of three
    component color values supplied as int parameters ranging from 0
    to 255: ``red``, ``green``, ``blue``.
