package com.facebook.presto.storage;

import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;

public interface StorageManager
{
    void insertSourceTable(NativeTableHandle tableHandle, QualifiedTableName sourceTableName);

    public QualifiedTableName getSourceTable(NativeTableHandle tableHandle);

    public void dropSourceTable(NativeTableHandle tableHandle);
}
