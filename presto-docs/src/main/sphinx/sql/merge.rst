=====
MERGE
=====

Synopsis
--------

.. code-block:: text

    MERGE INTO target_table [ [ AS ]  target_alias ]
    USING { source_table | query } [ [ AS ] source_alias ]
    ON search_condition
    WHEN MATCHED THEN
        UPDATE SET ( column = expression [, ...] )
    WHEN MATCHED THEN
        DELETE
    WHEN NOT MATCHED THEN
        INSERT [ column_list ]
        VALUES (expression, ...)

Description
-----------

The ``MERGE`` statement conditionally inserts, updates, or deletes rows in a ``target_table`` based on the contents of the ``source_table``.
The ``search_condition`` defines a relation between the source and target tables.

When the condition is met, one of the following ``MATCHED`` actions can be taken:

* ``UPDATE``: The target row is updated. The ``UPDATE`` column value expressions can depend on any field of the target or the source.
* ``DELETE``: The target row is deleted from the target table.

When the condition is not met, the ``NOT MATCHED`` action inserts a new row into the target table. The ``INSERT`` expressions can depend on any field of the source.

A ``MERGE`` statement can contain any combination of ``WHEN MATCHED`` and ``WHEN NOT MATCHED`` clauses. For example, you can use ``WHEN MATCHED THEN DELETE`` together with ``WHEN NOT MATCHED THEN INSERT`` to delete existing matched rows and insert new unmatched rows in a single atomic operation.

The ``MERGE`` command requires each target row to match at most one source row. An exception is raised when a single target table row matches more than one source row.
If a source row is not matched by the ``WHEN MATCHED`` clause and there is no ``WHEN NOT MATCHED`` clause, the source row is ignored.

The ``MERGE`` statement is commonly used to integrate data from two tables with different contents but similar structures.
For example, the source table could be part of a production transactional system, while the target table might be located in a data warehouse for analytics.
Regularly, MERGE operations are performed to update the analytics warehouse with the latest production data.
You can also use MERGE with tables that have different structures, as long as you can define a condition to match the rows between them.

MERGE Command Privileges
------------------------

The ``MERGE`` statement does not have a dedicated privilege. Instead, executing a ``MERGE`` statement requires the privileges associated with the individual actions it performs:

* ``UPDATE`` actions: require the ``UPDATE`` privilege on the target table columns referenced in the ``SET`` clause.
* ``DELETE`` actions: require the ``DELETE`` privilege on the target table.
* ``INSERT`` actions: require the ``INSERT`` privilege on the target table.

Each privilege must be granted to the user executing the ``MERGE`` command, based on the specific operations included in the statement.

Examples
--------

Update and insert
^^^^^^^^^^^^^^^^^

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

Delete and insert
^^^^^^^^^^^^^^^^^

Delete matched rows from the target table and insert unmatched rows from the source. This is useful for replacing existing records with new data in a single atomic operation.

.. code-block:: text

    MERGE INTO product_sales AS s
        USING monthly_sales AS ms
        ON s.product_id = ms.product_id
    WHEN MATCHED THEN
        DELETE
    WHEN NOT MATCHED THEN
        INSERT (product_id, sales, last_sale, current_price)
        VALUES (ms.product_id, ms.sales, ms.sale_date, ms.price)

Delete only
^^^^^^^^^^^

Delete all rows in the target table that match the source. Rows in the target table that have no match in the source remain unchanged.

.. code-block:: text

    MERGE INTO product_sales AS s
        USING discontinued_products AS d
        ON s.product_id = d.product_id
    WHEN MATCHED THEN
        DELETE

Limitations
-----------

Any connector can be used as a source table for a ``MERGE`` statement.
Only connectors which support the ``MERGE`` statement can be the target of a merge operation.
See the :doc:`connector documentation </connector/>` for more information.
The ``MERGE`` statement is currently supported only by the Iceberg connector.
