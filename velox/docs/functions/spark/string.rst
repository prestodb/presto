====================================
String Functions
====================================

Unless specified otherwise, all functions return NULL if at least one of the arguments is NULL.

.. spark:function:: ascii(string) -> integer

    Returns unicode code point of the first character of ``string``. Returns 0 if ``string`` is empty.

.. spark:function:: bit_length(string/binary) -> integer

    Returns the bit length for the specified string column. ::
        
        SELECT bit_length('123'); -- 24

.. spark:function:: chr(n) -> varchar

    Returns the Unicode code point ``n`` as a single character string.
    If ``n < 0``, the result is an empty string.
    If ``n >= 256``, the result is equivalent to chr(``n % 256``).

.. spark:function:: contains(left, right) -> boolean

    Returns true if 'right' is found in 'left'. Otherwise, returns false. ::
        
        SELECT contains('Spark SQL', 'Spark'); -- true
        SELECT contains('Spark SQL', 'SPARK'); -- false
        SELECT contains('Spark SQL', null); -- NULL
        SELECT contains(x'537061726b2053514c', x'537061726b'); -- true

.. spark:function:: conv(number, fromBase, toBase) -> varchar

    Converts ``number`` represented as a string from ``fromBase`` to ``toBase``.
    ``fromBase`` must be an INTEGER value between 2 and 36 inclusively. ``toBase`` must
    be an INTEGER value between 2 and 36 inclusively or between -36 and -2 inclusively.
    Otherwise, returns NULL.
    Returns a signed number if ``toBase`` is negative. Otherwise, returns an unsigned one.
    Returns NULL if ``number`` is empty.
    Skips leading spaces. ``number`` may contain other characters not valid for ``fromBase``.
    All characters starting from the first invalid character till the end of the string are
    ignored. Only converts valid characters even though ``fromBase`` = ``toBase``. Returns
    '0' if no valid character is found. ::

        SELECT conv('100', 2, 10); -- '4'
        SELECT conv('-10', 16, -10); -- '-16'
        SELECT conv("-1", 10, 16); -- 'FFFFFFFFFFFFFFFF'
        SELECT conv("123", 10, 39); -- NULL
        SELECT conv('', 16, 10); -- NULL
        SELECT conv(' ', 2, 10); -- NULL
        SELECT conv("11", 10, 16); -- 'B'
        SELECT conv("11ABC", 10, 16); -- 'B'
        SELECT conv("11abc", 10, 10); -- '11'
        SELECT conv('H016F', 16, 10); -- '0'

.. spark:function:: endswith(left, right) -> boolean

    Returns true if 'left' ends with 'right'. Otherwise, returns false. ::

        SELECT endswith('js SQL', 'SQL'); -- true
        SELECT endswith('js SQL', 'js'); -- false
        SELECT endswith('js SQL', NULL); -- NULL

.. spark:function:: find_in_set(str, strArray) -> integer

    Returns 1-based index of the given string ``str`` in the comma-delimited list ``strArray``.
    Returns 0, if the string was not found or if the given string ``str`` contains a comma. ::

        SELECT find_in_set('ab', 'abc,b,ab,c,def'); -- 3
        SELECT find_in_set('ab,', 'abc,b,ab,c,def'); -- 0
        SELECT find_in_set('dfg', 'abc,b,ab,c,def'); -- 0
        SELECT find_in_set('', ''); -- 1
        SELECT find_in_set('', '123,'); -- 2
        SELECT find_in_set('', ',123'); -- 1
        SELECT find_in_set(NULL, ',123'); -- NULL
        SELECT find_in_set("abc", NULL); -- NULL

.. spark:function:: instr(string, substring) -> integer

    Returns the starting position of the first instance of ``substring`` in
    ``string``. Positions start with ``1``. Returns 0 if 'substring' is not found.

.. spark:function:: left(string, length) -> string

    Returns the leftmost length characters from the ``string``.
    If ``length`` is less or equal than 0 the result is an empty string.

.. spark:function:: length(string) -> integer

    Returns the length of ``string`` in characters.

.. spark:function:: levenshtein(string1, string2[, threshold]) -> integer

    Returns the `Levenshtein distance <https://en.wikipedia.org/wiki/Levenshtein_distance>`_ between the two given strings.
    If the provided ``threshold`` is negative, or the levenshtein distance exceeds ``threshold``, returns -1. ::

        SELECT levenshtein('kitten', 'sitting'); -- 3
        SELECT levenshtein('kitten', 'sitting', 10); -- 3
        SELECT levenshtein('kitten', 'sitting', 2); -- -1

.. spark:function:: lower(string) -> string

    Returns string with all characters changed to lowercase. ::

        SELECT lower('SparkSql'); -- sparksql

.. spark:function:: lpad(string, len, pad) -> string
    
    Returns ``string``, left-padded with pad to a length of ``len``. If ``string`` is
    longer than ``len``, the return value is shortened to ``len`` characters or bytes.
    If ``pad`` is not specified, ``string`` will be padded to the left with space characters
    if it is a character string, and with zeros if it is a byte sequence. ::

        SELECT lpad('hi', 5, '??'); -- ???hi
        SELECT lpad('hi', 1, '??'); -- h
        SELECT lpad('hi', 4); --   hi

.. spark:function:: ltrim(string) -> varchar

    Removes leading 0x20(space) characters from ``string``. ::

        SELECT ltrim('  data  '); -- "data  "

.. spark:function:: ltrim(trimCharacters, string) -> varchar
   :noindex:

    Removes specified leading characters from ``string``. The specified character
    is any character contained in ``trimCharacters``.
    ``trimCharacters`` can be empty and may contain duplicate characters. ::

        SELECT ltrim('ps', 'spark'); -- "ark"

.. spark:function:: overlay(input, replace, pos, len) -> same as input

    Replace a substring of ``input`` starting at ``pos`` character with ``replace`` and
    going for rest ``len`` characters of ``input``.
    Types of ``input`` and ``replace`` must be the same. Supported types are: VARCHAR and VARBINARY.
    When ``input`` types are VARCHAR, ``len`` and ``pos`` are specified in characters, otherwise, bytes.
    Result is constructed from three parts.
    First part is first pos - 1 characters of ``input`` when ``pos`` if greater then zero, otherwise, empty string.
    Second part is ``replace``.
    Third part is rest of ``input`` from indices pos + len which is 1-based,
    if ``len`` is negative, it will be set to size of ``replace``,
    if pos + len is negative, it refers to -(pos + len)th element before the end of ``input``.
    ::

        SELECT overlay('Spark SQL', '_', 6, -1); -- "Spark_SQL"
        SELECT overlay('Spark SQL', 'CORE', 7, -1); -- "Spark CORE"
        SELECT overlay('Spark SQL', 'ANSI ', 7, 0); -- "Spark ANSI SQL"
        SELECT overlay('Spark SQL', 'tructured', 2, 4); -- "Structured SQL"
        SELECT overlay('Spark SQL', '_', -6, 3); -- "_Sql"

.. spark:function:: repeat(input, n) -> varchar

    Returns the string which repeats ``input`` ``n`` times. 
    Result size must be less than or equal to 1MB.
    If ``n`` is less than or equal to 0, empty string is returned. ::

        SELECT repeat('123', 2); -- 123123

.. spark:function:: replace(input, replaced) -> varchar

    Removes all instances of ``replaced`` from ``input``.
    If ``replaced`` is an empty string, returns the original ``input`` string. ::

        SELECT replace('ABCabc', ''); -- ABCabc
        SELECT replace('ABCabc', 'bc'); -- ABCc

.. spark:function:: replace(input, replaced, replacement) -> varchar

    Replaces all instances of ``replaced`` with ``replacement`` in ``input``.
    If ``replaced`` is an empty string, returns the original ``input`` string. ::

        SELECT replace('ABCabc', '', 'DEF'); -- ABCabc
        SELECT replace('ABCabc', 'abc', ''); -- ABC
        SELECT replace('ABCabc', 'abc', 'DEF'); -- ABCDEF

.. spark:function:: reverse(string) -> varchar

    Returns input string with characters in reverse order.

.. spark:function:: rpad(string, len, pad) -> string
    
    Returns ``string``, right-padded with ``pad`` to a length of ``len``. 
    If ``string`` is longer than ``len``, the return value is shortened to ``len`` characters.
    If ``pad`` is not specified, ``string`` will be padded to the right with space characters
    if it is a character string, and with zeros if it is a binary string. ::

        SELECT lpad('hi', 5, '??'); -- ???hi
        SELECT lpad('hi', 1, '??'); -- h
        SELECT lpad('hi', 4); -- hi  

.. spark:function:: rtrim(string) -> varchar

    Removes trailing 0x20(space) characters from ``string``. ::

        SELECT rtrim('  data  '); -- "  data"

.. spark:function:: rtrim(trimCharacters, string) -> varchar
   :noindex:

    Removes specified trailing characters from ``string``. The specified character
    is any character contained in ``trimCharacters``.
    ``trimCharacters`` can be empty and may contain duplicate characters. ::

        SELECT rtrim('kr', 'spark'); -- "spa"

.. spark:function:: soundex(string) -> string

    Returns `Soundex code <https://en.wikipedia.org/wiki/Soundex>`_ of the string. If first character of ``string`` is not
    a letter, ``string`` is returned. ::

        SELECT soundex('Miller'); -- "M460"

.. spark:function:: split(string, delimiter) -> array(string)

    Splits ``string`` on ``delimiter`` and returns an array. ::

        SELECT split('oneAtwoBthreeC', '[ABC]'); -- ["one","two","three",""]
        SELECT split('one', ''); -- ["o", "n", "e", ""]
        SELECT split('one', '1'); -- ["one"]

.. spark:function:: split(string, delimiter, limit) -> array(string)
   :noindex:

    Splits ``string`` on ``delimiter`` and returns an array of size at most ``limit``. ::

        SELECT split('oneAtwoBthreeC', '[ABC]', -1); -- ["one","two","three",""]
        SELECT split('oneAtwoBthreeC', '[ABC]', 0); -- ["one", "two", "three", ""]
        SELECT split('oneAtwoBthreeC', '[ABC]', 2); -- ["one","twoBthreeC"]

.. spark:function:: startswith(left, right) -> boolean

    Returns true if 'left' starts with 'right'. Otherwise, returns false. ::

        SELECT startswith('js SQL', 'js'); -- true
        SELECT startswith('js SQL', 'SQL'); -- false
        SELECT startswith('js SQL', null); -- NULL

.. spark:function:: str_to_map(string, entryDelimiter, keyValueDelimiter) -> map(string, string)

    Returns a map by splitting ``string`` into entries with ``entryDelimiter`` and splitting
    each entry into key/value with ``keyValueDelimiter``.
    ``entryDelimiter`` and ``keyValueDelimiter`` must be constant strings with single ascii
    character. Allows ``keyValueDelimiter`` not found when splitting an entry. Throws exception
    when duplicate map keys are found for single row's result, consistent with Spark's default
    behavior. ::

        SELECT str_to_map('a:1,b:2,c:3', ',', ':'); -- {"a":"1","b":"2","c":"3"}
        SELECT str_to_map('a', ',', ':'); -- {"a":NULL}
        SELECT str_to_map('', ',', ':'); -- {"":NULL}
        SELECT str_to_map('a:1,b:2,c:3', ',', ','); -- {"a:1":NULL,"b:2":NULL,"c:3":NULL}

.. spark:function:: substring(string, start) -> varchar

    Returns the rest of ``string`` from the starting position ``start``.
    Positions start with ``1``. A negative starting position is interpreted
    as being relative to the end of the string. When the starting position is 0,
    the meaning is to refer to the first character.Type of 'start' must be an INTEGER. 

.. spark:function:: substring(string, start, length) -> varchar
   :noindex:

    Returns a substring from ``string`` of length ``length`` from the starting
    position ``start``. Positions start with ``1``. A negative starting
    position is interpreted as being relative to the end of the string.
    When the starting position is 0, the meaning is to refer to the first character.
    Type of 'start' must be an INTEGER. ::

        SELECT substring('Spark SQL', 0, 2); -- Sp
        SELECT substring('Spark SQL', 1, 2); -- Sp
        SELECT substring('Spark SQL', 5, 0); -- ""
        SELECT substring('Spark SQL', 5, -1); -- ""
        SELECT substring('Spark SQL', 5, 10000); -- "k SQL"
        SELECT substring('Spark SQL', -9, 3); -- "Spa"
        SELECT substring('Spark SQL', -10, 3); -- "Sp"
        SELECT substring('Spark SQL', -20, 3); -- ""

.. spark:function:: substring_index(string, delim, count) -> [same as string]

    Returns the substring from ``string`` before ``count`` occurrences of the delimiter ``delim``.
    Here the ``string`` can be VARCHAR or VARBINARY and return type matches type of ``string``.
    If ``count`` is positive, returns everything to the left of the final delimiter
    (counting from the left). If ``count`` is negative, returns everything to the right
    of the final delimiter (counting from the right). If ``count`` is 0, returns empty string.
    If ``delim`` is not found or found fewer times than ``count``, returns the original input string.
    ``delim`` is case-sensitive. It also takes into account overlapping strings. ::

        SELECT substring_index('Spark.SQL', '.', 1); -- "Spark"
        SELECT substring_index('Spark.SQL', '.', 0); -- ""
        SELECT substring_index('Spark.SQL', '.', -1); -- "SQL"
        SELECT substring_index('TEST.Spark.SQL', '.',2); -- "TEST.Spark"
        SELECT substring_index('TEST.Spark.SQL', '', 0); -- ""
        SELECT substring_index('TEST.Spark.SQL', '.', -2); -- "Spark.SQL"
        SELECT substring_index('TEST.Spark.SQL', '.', 10); -- "TEST.Spark.SQL"
        SELECT substring_index('TEST.Spark.SQL', '.', -12); -- "TEST.Spark.SQL"
        SELECT substring_index('aaaaa', 'aa', 2); -- "a"
        SELECT substring_index('aaaaa', 'aa', -4); -- "aaa"
        SELECT substring_index('aaaaa', 'aa', 0); -- ""
        SELECT substring_index('aaaaa', 'aa', 5); -- "aaaaa"
        SELECT substring_index('aaaaa', 'aa', -5); -- "aaaaa"

.. spark:function:: translate(string, match, replace) -> varchar

    Returns a new translated string. It translates the character in ``string`` by a
    character in ``replace``. The character in ``replace`` is corresponding to
    the character in ``match``. The translation will happen when any character
    in ``string`` matching with a character in ``match``. If ``match's`` character
    size is larger than ``replace's``, the extra characters in ``match`` will be
    removed from ``string``. In addition, this function only considers the first
    occurrence of a character in ``match`` and uses its corresponding character in
    ``replace`` for translation. ::

        SELECT translate('spark', 'sa', '12');  -- "1p2rk"
        SELECT translate('spark', 'sa', '1');   -- "1prk"
        SELECT translate('spark', 'ss', '12');  -- "1park"

.. spark:function:: trim(string) -> varchar

    Removes leading and trailing 0x20(space) characters from ``string``. ::

        SELECT trim('  data  '); -- "data"

.. spark:function:: trim(trimCharacters, string) -> varchar
   :noindex:

    Removes specified leading and trailing characters from ``string``.
    The specified character is any character contained in ``trimCharacters``.
    ``trimCharacters`` can be empty and may contain duplicate characters. ::

        SELECT trim('sprk', 'spark'); -- "a"

.. spark:function:: upper(string) -> string

    Returns string with all characters changed to uppercase. ::

        SELECT upper('SparkSql'); -- SPARKSQL