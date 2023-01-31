*********************
September 2021 Update
*********************

Documentation
-------------

* Add new articles: :doc:`Vectors <../develop/vectors>` and
  :doc:`Joins <../develop/joins>`.
* Add Presto functions coverage maps: :doc:`all <../functions/presto/coverage>`
  and :doc:`most used <../functions/presto/most_used_coverage>`.

Core Library
------------

* Add support for converting flat Velox vectors of primitive types to Arrow arrays.
* Add support for round-robin and Hive-compatible partitioning.
* Add support for LEFT OUTER and CROSS joins.
* Add EnforceSingleRow operator to support uncorrelated subqueries.
* Add function registry API.
* Enable dynamic filter pushdown for semi joins.
* Enable all-ASCII fast path for all eligible string functions.
* Fix the long standing problem of constant vectors not knowing their size and delete kMaxElements constant.
* Add ExpressionFuzzer tests to CircleCI and increase coverage.

Presto Functions
----------------

* Add :func:`array_distinct`, :func:`array_except`, reverse(array) functions.
* Add :func:`millisecond` function.
* Add :func:`trim`, :func:`ltrim`, :func:`rtrim` functions.
* Add :func:`sin`, :func:`asin`, :func:`cos`, :func:`acos`, :func:`tan`,
  :func:`atan`, :func:`atan2`, :func:`cosh`, :func:`tanh` functions.
* Add :func:`to_utf8`, :func:`url_encode`, :func:`url_decode`,
  :func:`to_base64`, :func:`from_base64`, :func:`to_hex`, :func:`from_hex` functions..
* Add BETWEEN operator for strings.
* Add :func:`stddev_pop`, :func:`stddev_samp`, :func:`var_pop`, :func:`var_samp`
  aggregate Presto functions.
* Add timestamp with time zone Presto type.

Credits
-------

Adam Simpkins, Aditi Pandit, Amit Dutta, Andy Lee, Aniket Mokashi, Anuradha
Weeraman, Chao Chen, Christy Lee-Eusman (PL&R), Darren Fu, Deepak Majeti, Huameng Jiang, Jake
Jung, Jialiang Tan, Krishna Pai, MJ Deng, Masha Basmanova, Michael Shang, Orri
Erling, Pedro Eugenio Rocha Pedreira, Ravindra Sunkad, Rob Kinyon, Sagar
Mittal, Sarah Li, Sergey Pershin, Sourav Kumar, Stefan Roesch, Wenlei Xie, Yuan
Chao Chou, Yue Yin, Zeyi (Rice) Fan, Zhengchao Liu, Zhenyuan Zhao, amaliujia,
ienkovich, miaoever.
