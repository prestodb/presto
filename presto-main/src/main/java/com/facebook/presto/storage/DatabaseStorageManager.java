package com.facebook.presto.storage;

import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.storage.StorageDao.Utils;
import com.google.inject.Inject;
import org.skife.jdbi.v2.IDBI;

import static com.google.common.base.Preconditions.checkNotNull;

public class DatabaseStorageManager
        implements StorageManager
{
    private final IDBI dbi;
    private final StorageDao dao;

    @Inject
    DatabaseStorageManager(@ForStorage IDBI dbi)
            throws InterruptedException
    {
        this.dbi = checkNotNull(dbi, "dbi is null");
        this.dao = dbi.onDemand(StorageDao.class);

        Utils.createStorageTablesWithRetry(dao);
    }

    @Override
    public void insertTableSource(NativeTableHandle tableHandle,
            QualifiedTableName sourceTableName)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(sourceTableName, "sourceTableName is null");

        dao.insertSourceTable(tableHandle.getTableId(), sourceTableName);
    }

    @Override
    public QualifiedTableName getTableSource(NativeTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");

        return dao.getSourceTable(tableHandle.getTableId());
    }

    @Override
    public void dropTableSource(NativeTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");

        dao.dropSourceTable(tableHandle.getTableId());
    }
}
