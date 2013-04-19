package com.facebook.presto.storage;

import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;

/**
 * Handles the sources for materialized view.
 *
 * TODO (after discussion). These methods should be part of the metadata manager and the metadata manager
 * should handle resolution of table handles (and information storage).
 *
 * This change should be made after the big import client / metadata changes went in.
 */
public interface StorageManager
{
    void insertTableSource(NativeTableHandle tableHandle, QualifiedTableName sourceTableName);

    public QualifiedTableName getTableSource(NativeTableHandle tableHandle);

    public void dropTableSource(NativeTableHandle tableHandle);
}
