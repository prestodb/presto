*******************
October 2021 Update
*******************

Documentation
-------------

* Add new article: :doc:`Plan Nodes and Operators </develop/operators>`.

Core Library
------------

* Enhance simple functions to

  * Allow returning an array of strings.
  * Process constant inputs once.
  * Access query configs.
  * Allow for providing fast path for all-ASCII inputs.

  More details in :doc:`How to add a scalar function? </develop/scalar-functions>` guide.
* Add support for RIGHT OUTER joins.
* Add DATE type.
* Add FixedSizeArray vector.
* Add support for reading DWRF files from `AWS S3 <https://aws.amazon.com/pm/serv-s3/>`_ API compatible file systems, e.g. Minio and AWS-S3.
* Add support for reading Parquet files using `DuckDB’s Parquet reader <https://duckdb.org/2021/06/25/querying-parquet.html>`_.

Presto Functions
----------------

* Add all remaining bitwise_xxx functions.
* Add :func:`greatest` and :func:`least` functions.
* Add :func:`log2` and :func:`log10` functions.
* Add :func:`date_trunc`, :func:`day_of_week`, :func:`day_of_year`, :func:`year`, :func:`month`, :func:`day`, :func:`minute`, :func:`hour`, :func:`second` functions.
* Add :func:`regexp_extract_all` function.
* Add reverse(varchar), :func:`split` functions.
* Add LIKE operator.

Credits
-------

Aditi Pandit, Amit Dutta, Atanu Ghosh, Behnam Robatmili, Bo Huang, Cooper Lees, Damian
Reeves, Darren Fu, Deepak Majeti, Ethan Xue, Genevieve Helsel, Giuseppe
Ottaviano, Huameng Jiang, Jake Jung, Jonathan Mendoza, Jun Wu, Kevin Wilfong, Krishna
Pai, Laith Sakka, MJ Deng, Marko Vuksanovic, Marshall, Masha Basmanova, Michael
Shang, Orri Erling, Pedro Eugenio Rocha Pedreira, Sagar Mittal, Sergey
Pershin, Shashank Chaudhry, Wei He, Wenlei Xie, Yating Zhou, Yedidya Feldblum, Yoav
Helfman, Zhaobo Liu, Zhenyuan Zhao, amaliujia, chang.chen, ienkovich.
