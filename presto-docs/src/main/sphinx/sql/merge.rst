==========
MERGE INTO
==========

Synopsis
--------

.. code-block:: text

    MERGE INTO target_table [ [ AS ]  target_alias ]
    USING { source_table | query } [ [ AS ] source_alias ]
    ON search_condition
    WHEN MATCHED THEN
        UPDATE SET ( column = expression [, ...] )
    WHEN NOT MATCHED THEN
        INSERT [ column_list ]
        VALUES (expression, ...)

Description
-----------

The ``MERGE INTO``` statement inserts or updates rows in a ``target_table`` based on the contents of the ``source_table``.
The ``search_condition`` defines a relation between the source and target tables.
When the condition is met, the target row is updated. When the condition is not met, a new row is inserted into the target table.
In the ``MATCHED`` case, the ``UPDATE`` column value expressions can depend on any field of the target or the source.
In the ``NOT MATCHED`` case, the ``INSERT`` expressions can depend on any field of the source.

A ``MERGE_TARGET_ROW_MULTIPLE_MATCHES`` exception is raised when a single target table row matches more than one source row.
If a source row is not matched by the ``WHEN`` clause and there is no ``WHEN NOT MATCHED`` clause, the source row is ignored.

The ``MERGE INTO`` statement is commonly used to integrate data from two tables with different contents but similar structures.
For example, the source table could be part of a production transactional system, while the target table might be located in a data warehouse for analytics.
Regularly, MERGE operations are performed to update the analytics warehouse with the latest production data.
You can also use MERGE with tables that have different structures, as long as you can define a condition to match the rows between them.

Example
-------

Update the sales information for existing products and insert the sales information for the new products in the market.

.. code-block:: text

    MERGE INTO product_sales AS s
        USING monthly_sales AS ms
        ON s.product_id = ms.product_id
    WHEN MATCHED THEN
        UPDATE SET
            sales = sales + ms.sales
          , last_sale = ms.sale_date
          , current_price = ms.price
    WHEN NOT MATCHED THEN
        INSERT (product_id, sales, last_sale, current_price)
        VALUES (ms.product_id, ms.sales, ms.sale_date, ms.price)

Limitations
-----------

Any connector can be used as a source table for a ``MERGE INTO`` statement.
Only connectors which support the ``MERGE INTO`` statement can be the target of a merge operation.
See the :doc:`connector documentation </connector/>` for more information.
