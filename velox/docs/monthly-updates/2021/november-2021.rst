********************
November 2021 Update
********************

Documentation
-------------

* Add :doc:`How to add lambda function? </develop/lambda-functions>` guide.

Core Library
------------

* Add basic support for inner merge join for the case when join inputs are sorted on the join keys.
* Add AssignUniqueId operator to enable TPC-H query 21.
* Optimize simple functions for inputs of type ARRAY and MAP.

Presto Functions
----------------

* Add :func:`infinity`, :func:`is_finite`, :func:`is_infinite`, :func:`nan`, :func:`is_nan` functions.
* Add :func:`parse_datetime`, :func:`quarter`, :func:`year_of_week`, :func:`yow` functions.
* Add :func:`array_duplicates`, :func:`slice`, :func:`zip` functions.
* Add :func:`rpad`, :func:`lpad`, :func:`split_part` functions.
* Add :func:`url_extract_host`, :func:`url_extract_fragment`, :func:`url_extract_path`,
  :func:`url_extract_parameter`, :func:`url_extract_protocol`, :func:`url_extract_port`,
  :func:`url_extract_query` functions.
* Add :func:`approx_set`, :func:`empty_approx_set`, :func:`merge` and :func:`cardinality` functions.
* Add support for DATE inputs to date extraction functions to enable TPC-H queries 7, 8, and 9.

Credits
-------

Abhash Jain, Aditi Pandit, Alex Hornby, Andy Lee, Behnam Robatmili, Chad Austin,
Chao Chen, Darren Fu, David Kang, Deepak Majeti, Huameng Jiang, Jake Jung,
Jialiang Tan, Jialing Zhou, Justin Yang, Kevin Wilfong, Konstantin Tsoy,
Krishna Pai, Laith Sakka, MJ Deng, Masha Basmanova, Michael Shang, Naresh
Kumar, Orri Erling, Pedro Eugenio Rocha Pedreira, Thomas Orozco, Wei He, Yating
Zhou, Yuan Chao Chou, Zhenyuan Zhao, frankobe, ienkovich.