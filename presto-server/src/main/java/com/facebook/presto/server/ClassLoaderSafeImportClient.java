/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.SchemaTableMetadata;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

@SuppressWarnings("UnusedDeclaration")
public class ClassLoaderSafeImportClient
        implements ImportClient
{
    private final ImportClient delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeImportClient(ImportClient delegate, ClassLoader classLoader)
    {
        Preconditions.checkNotNull(delegate, "delegate is null");
        Preconditions.checkNotNull(classLoader, "classLoader is null");
        this.delegate = delegate;
        this.classLoader = classLoader;
    }

    @Override
    public String getConnectorId()
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getConnectorId();
        }
    }

    @Override
    public List<String> listSchemaNames()
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.listSchemaNames();
        }
    }

    @Override
    public List<SchemaTableName> listTables(String schemaNameOrNull)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.listTables(schemaNameOrNull);
        }
    }

    @Override
    public TableHandle getTableHandle(SchemaTableName tableName)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableHandle(tableName);
        }
    }

    @Override
    public SchemaTableMetadata getTableMetadata(TableHandle table)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableMetadata(table);
        }
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getColumnHandle(tableHandle, columnName);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getColumnHandles(tableHandle);
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getColumnMetadata(tableHandle, columnHandle);
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.listTableColumns(prefix);
        }
    }

    @Override
    public TableHandle createTable(SchemaTableMetadata tableMetadata)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.createTable(tableMetadata);
        }
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            delegate.dropTable(tableHandle);
        }
    }

    @Override
    public List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getPartitions(table, bindings);
        }
    }

    @Override
    public Iterable<Split> getPartitionSplits(List<Partition> partitions)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getPartitionSplits(partitions);
        }
    }

    @Override
    public RecordSet getRecordSet(Split split, List<? extends ColumnHandle> columns)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getRecordSet(split, columns);
        }
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.canHandle(tableHandle);
        }
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.canHandle(columnHandle);
        }
    }

    @Override
    public boolean canHandle(Split split)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.canHandle(split);
        }
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getColumnHandleClass();
        }
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableHandleClass();
        }
    }

    @Override
    public Class<? extends Split> getSplitClass()
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getSplitClass();
        }
    }
}
