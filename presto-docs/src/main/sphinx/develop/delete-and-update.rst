====================================
Supporting ``DELETE`` and ``UPDATE``
====================================

The Presto engine provides APIs to support row-level SQL ``DELETE`` and ``UPDATE``.
To implement ``DELETE`` or ``UPDATE``, a connector must:

* Layer an ``UpdatablePageSource`` on top of the connector's ``ConnectorPageSource``
* Define ``ConnectorMetadata`` methods to get a rowId column handle
* Start the operation using ``beginUpdate()`` or ``beginDelete()``
* Finish the operation using ``finishUpdate()`` or ``finishDelete()``

``DELETE`` and ``UPDATE`` Data Flow
===================================

``DELETE`` and ``UPDATE`` have a similar flow:

* For each split, the connector will create an ``UpdatablePageSource`` instance, layered over the
  connector's ``ConnectorPageSource``, to read pages on behalf of the Presto engine, and to
  write deletions or updates to the underlying data store.
* The connector's ``UpdatablePageSource.getNextPage()`` implementation fetches the next page
  from the underlying ``ConnectorPageSource``, optionally reformats the page, and returns it
  to the Presto engine.
* The Presto engine performs filtering and projection on the page read, producing a page of filtered,
  projected results.
* The Presto engine passes that filtered, projected page of results to the connector's
  ``UpdatablePageSource`` ``deleteRows()`` or ``updateRows()`` method.  Those methods persist
  the deletions or updates in the underlying data store.
* When all the pages for a specific split have been processed, the Presto engine calls
  ``UpdatablePageSource.finish()``, which returns a ``Collection<Slice>`` of fragments
  representing connector-specific information about the rows processed by the calls to
  ``deleteRows`` or ``updateRows``.
* When all pages for all splits have been processed, the Presto engine calls ``ConnectorMetadata.finishDelete()`` or
  ``finishUpdate``, passing a collection containing all the fragments from all the splits.  The connector
  does what is required to finalize the operation, for example, committing the transaction.

The rowId Column Handle Abstraction
===================================

The Presto engine and connectors use a rowId column handle abstraction to agree on the identities of rows
to be updated or deleted.  The rowId column handle is opaque to the Presto engine.  Depending on the connector,
the rowId column handle abstraction could represent several physical columns.

The rowId Column Handle for ``DELETE``
--------------------------------------

The Presto engine identifies the rows to be deleted using a connector-specific
rowId column handle, returned by the connector's ``ConnectorMetadata.getDeleteRowIdColumnHandle()``
method, whose full signature is::

    ColumnHandle getDeleteRowIdColumnHandle(
        ConnectorSession session,
        ConnectorTableHandle tableHandle)

The rowId Column Handle for ``UPDATE``
--------------------------------------

The Presto engine identifies rows to be updated using a connector-specific rowId column handle,
returned by the connector's ``ConnectorMetadata.getUpdateRowIdColumnHandle()``
method.  In addition to the columns that identify the row, for ``UPDATE`` the rowId column will contain
any columns that the connector requires in order to perform the ``UPDATE`` operation.

UpdatablePageSource API
=======================

As mentioned above, to support ``DELETE`` or ``UPDATE``, the connector must define a subclass of
``UpdatablePageSource``, layered over the connector's ``ConnectorPageSource``.  The interesting methods are:

* ``Page getNextPage()``.  When the Presto engine calls ``getNextPage()``, the ``UpdatablePageSource`` calls
  its underlying ``ConnectorPageSource.getNextPage()`` method to get a page.  Some connectors will reformat
  the page before returning it to the Presto engine.

* ``void deleteRows(Block rowIds)``.  The Presto engine calls the ``deleteRows()`` method of the same ``UpdatablePageSource``
  instance that supplied the original page,  passing a block of rowIds, created by the Presto engine based on the column
  handle returned by ``ConnectorMetadata.getDeleteRowIdColumnHandle()``

* ``void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)``.  The Presto engine calls the ``updateRows()``
  method of the same ``UpdatablePageSource`` instance that supplied the original page, passing a page of projected columns,
  one for each updated column and the last one for the rowId column.  The order of projected columns is defined by the Presto engine,
  and that order is reflected in the ``columnValueAndRowIdChannels`` argument.  The job of ``updateRows()`` is to:

  * Extract the updated column blocks and the rowId block from the projected page.
  * Assemble them in whatever order is required by the connector for storage.
  * Store the update result in the underlying file store.

* ``CompletableFuture<Collection<Slice>> finish()``.  The Presto engine calls ``finish()`` when all the pages
  of a split have been processed.  The connector returns a future containing a collection of ``Slice``, representing
  connector-specific information about the rows processed.  Usually this will include the row count, and might
  include information like the files or partitions created or changed.

``ConnectorMetadata`` ``DELETE`` API
====================================

A connector implementing ``DELETE`` must specify three ``ConnectorMetadata`` methods.

* ``getDeleteRowIdColumnHandle()``::

   ColumnHandle getDeleteRowIdColumnHandle(
        ConnectorSession session,
        ConnectorTableHandle tableHandle)

  The ColumnHandle returned by this method provides the rowId column handle used by the connector to identify rows
  to be deleted, as well as any other fields of the row that the connector will need to complete the ``DELETE`` operation.
  For a JDBC connector, that rowId is usually the primary key for the table and no other fields are required.
  For other connectors, the information needed to identify a row usually consists of multiple physical columns.

* ``beginDelete()``::

    ConnectorDeleteTableHandle beginDelete(
         ConnectorSession session,
         ConnectorTableHandle tableHandle)

  As the last step in creating the ``DELETE`` execution plan, the connector's ``beginDelete()`` method is called,
  passing the ``session`` and ``tableHandle``.

  ``beginDelete()`` performs any orchestration needed in the connector to start processing the ``DELETE``.
  This orchestration varies from connector to connector.

  ``beginDelete()`` returns a ``ConnectorDeleteTableHandle`` with any added information the connector needs when the handle
  is passed back to ``finishDelete()`` and the split generation machinery.  For most connectors, the returned table
  handle contains a flag identifying the table handle as a table handle for a ``DELETE`` operation.

* ``finishDelete()``::

      void finishDelete(
          ConnectorSession session,
          ConnectoDeleteTableHandle tableHandle,
          Collection<Slice> fragments)

  During ``DELETE`` processing, the Presto engine accumulates the ``Slice`` collections returned by ``UpdatablePageSource.finish()``.
  After all splits have been processed, the engine calls ``finishDelete()``, passing the table handle and that
  collection of ``Slice`` fragments.  In response, the connector takes appropriate actions to complete the ``Delete`` operation.
  Those actions might include committing the transaction, assuming the connector supports a transaction paradigm.

``ConnectorMetadata`` ``UPDATE`` API
====================================

A connector implementing ``UPDATE`` must specify three ``ConnectorMetadata`` methods.

* ``getUpdateRowIdColumnHandle``::

   ColumnHandle getUpdateRowIdColumnHandle(
        ConnectorSession session,
        ConnectorTableHandle tableHandle,
        List<ColumnHandle> updatedColumns)

  The ``updatedColumns`` list contains column handles for all columns updated by the ``UPDATE`` operation in table column order.

  The ColumnHandle returned by this method provides the rowId used by the connector to identify rows to be updated, as
  well as any other fields of the row that the connector will need to complete the ``UPDATE`` operation.

* ``beginUpdate``::

    ConnectorTableHandle beginUpdate(
         ConnectorSession session,
         ConnectorTableHandle tableHandle,
         List<ColumnHandle> updatedColumns)

  As the last step in creating the ``UPDATE`` execution plan, the connector's ``beginUpdate()`` method is called,
  passing arguments that define the ``UPDATE`` to the connector.  In addition to the ``session``
  and ``tableHandle``, the arguments includes the list of the updated columns handles, in table column order.

  ``beginUpdate()`` performs any orchestration needed in the connector to start processing the ``UPDATE``.
  This orchestration varies from connector to connector.

  ``beginUpdate`` returns a ``ConnectorTableHandle`` with any added information the connector needs when the handle
  is passed back to ``finishUpdate()`` and the split generation machinery.  For most connectors, the returned table
  handle contains a flag identifying the table handle as a table handle for a ``UPDATE`` operation.  For some connectors
  that support partitioning, the table handle will reflect that partitioning.

* ``finishUpdate``::

      void finishUpdate(
          ConnectorSession session,
          ConnectorTableHandle tableHandle,
          Collection<Slice> fragments)

  During ``UPDATE`` processing, the Presto engine accumulates the ``Slice`` collections returned by ``UpdatablePageSource.finish()``.
  After all splits have been processed, the engine calls ``finishUpdate()``, passing the table handle and that
  collection of ``Slice`` fragments.  In response, the connector takes appropriate actions to complete the ``UPDATE`` operation.
  Those actions might include committing the transaction, assuming the connector supports a transaction paradigm.
