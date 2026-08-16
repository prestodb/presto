===================
CREATE VECTOR INDEX
===================

Synopsis
--------

.. code-block:: none

    CREATE VECTOR INDEX index_name
    ON source_table ( column_name [, ...] )
    [ WITH ( property_name = expression [, ...] ) ]
    [ UPDATING FOR predicate ]

Description
-----------

Create a vector search index on the specified columns of a source table.

The ``column_name`` list specifies the columns to index. Typically this includes
an identifier column and an embedding (vector) column.

The optional ``WITH`` clause sets index properties such as:

- ``index_type``: The type of vector index (for example, ``'ivf_rabitq4'``)
- ``distance_metric``: The distance metric to use (for example, ``'cosine'``)
- ``index_options``: Additional index configuration (for example, ``'nlist=100000,nb=8'``)
- ``partitioned_by``: Partition columns (for example, ``ARRAY['ds']``)

The optional ``UPDATING FOR`` clause filters the source data using a predicate,
similar to a ``WHERE`` clause. This is typically used to specify which partitions
to build the index for.

Examples
--------

Create a vector index on an embeddings table for specific date partitions::

    CREATE VECTOR INDEX my_index
    ON my_table(content_id, embedding)
    WITH (
        index_type = 'ivf_rabitq4',
        distance_metric = 'cosine',
        index_options = 'nlist=100000,nb=8',
        partitioned_by = ARRAY['ds']
    )
    UPDATING FOR ds BETWEEN '2024-01-01' AND '2024-01-03'

See Also
--------

:doc:`create-table`, :doc:`create-table-as`
