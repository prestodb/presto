********************
January 2022 Update
********************

Core Library
------------

* Add support for bucket-by-bucket query execution to allow for lower memory
  footprint for joins and aggregations on bucketed-by keys.
* Add support for converting ROW vectors (structs) to and from Arrow arrays.
* Add support for variadic arguments to simple functions.
* Add support for LEFT merge join.
* Add support for AS statement to PlanBuilder.

Hive Connector
--------------

* Add support for AWS S3 IAM Roles.

Presto Functions
----------------

* Add :func:`pi`, :func:`to_base`, :func:`array_join`, :func:`regexp_replace`
  functions.

Credits
-------

Aditi Pandit, Alex Hornby, Andy Lee, Arun Thirupathi, Bikash Chandra, Chad
Austin, Chao Chen, Cheng Su, Damian Reeves, Deepak Majeti, Jake Jung, Kevin
Wilfong, Krishna Pai, Kunal Chakraborti, Laith Sakka, MJ Deng, Masha Basmanova,
Muir Manders, Naresh Kumar, Orri Erling, Pedro Eugenio Rocha Pedreira, Sergey
Pershin, Tom Jackson, Victor Zverovich, Wei He, Wei Zheng, Xavier Deguillard,
Ying, Zeyi (Rice) Fan, Zhenyuan Zhao, Zsolt Dollenstein, xuedongluan
