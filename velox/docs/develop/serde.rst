*********************
Serialization Formats
*********************

Velox supports three data serialization formats that can be used for data shuffle:
`PrestoPage <https://prestodb.io/docs/current/develop/serialized-page.html>`_,
UnsafeRow and CompactRow. PrestoPage is a columnar format. UnsafeRow and CompactRow
are row-wise formats.

Velox applications can register their own formats as well.

PrestoPage format is described in the `Presto documentation <https://prestodb.io/docs/current/develop/serialized-page.html>`_.

UnsafeRow format comes from `Apache Spark <https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-UnsafeRow.html>`_.

CompactRow is similar to UnsafeRow, but it is more space efficient and results in
fewer bytes shuffled which has a cascading effect on CPU usage (for compression
and checksumming) and memory (for buffering).

The details of UnsafeRow and CompactRow formats can be found in the following articles.

.. toctree::
    :maxdepth: 1

    serde/unsaferow
    serde/compactrow

Velox also uses another row-wise serialization format, ContainerRowSerde, for storing
data in aggregation and join operators. This format is similar to CompactRow.
