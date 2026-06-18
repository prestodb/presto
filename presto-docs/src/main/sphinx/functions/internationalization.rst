==============================
Internationalization Functions
==============================

Myanmar Functions
-----------------

.. note::

    Text written in Myanmar uses primarily the Zawgyi font encoding,
    which is not compatible with Unicode. These functions provide
    some utilities for comparing content in Myanmar despite the font
    encoding.

    See http://www.unicode.org/faq/myanmar.html for more information.

.. function:: myanmar_font_encoding(text) -> varchar

    Returns the font encoding of the text. Returns ``zawgyi`` if the content is Zawgyi encoded and ``unicode`` otherwise.

.. function:: myanmar_normalize_unicode(text) -> varchar

    Returns the text normalized as Unicode. Zawgyi text is converted to Unicode and Unicode text is left unchanged.
