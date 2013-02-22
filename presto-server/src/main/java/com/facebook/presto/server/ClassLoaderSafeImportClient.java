/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ObjectNotFoundException;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.spi.PartitionInfo;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaField;
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
    public List<String> getDatabaseNames()
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getDatabaseNames();
        }
    }

    @Override
    public List<String> getTableNames(String databaseName)
            throws ObjectNotFoundException
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableNames(databaseName);
        }
    }

    @Override
    public List<SchemaField> getTableSchema(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableSchema(databaseName, tableName);
        }
    }

    @Override
    public List<SchemaField> getPartitionKeys(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getPartitionKeys(databaseName, tableName);
        }
    }

    @Override
    public List<PartitionInfo> getPartitions(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getPartitions(databaseName, tableName);
        }
    }

    @Override
    public List<PartitionInfo> getPartitions(String databaseName, String tableName, Map<String, Object> filters)
            throws ObjectNotFoundException
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getPartitions(databaseName, tableName, filters);
        }
    }

    @Override
    public List<String> getPartitionNames(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getPartitionNames(databaseName, tableName);
        }
    }

    @Override
    public Iterable<PartitionChunk> getPartitionChunks(String databaseName, String tableName, String partitionName, List<String> columns)
            throws ObjectNotFoundException
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getPartitionChunks(databaseName, tableName, partitionName, columns);
        }
    }

    @Override
    public Iterable<PartitionChunk> getPartitionChunks(String databaseName, String tableName, List<String> partitionNames, List<String> columns)
            throws ObjectNotFoundException
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getPartitionChunks(databaseName, tableName, partitionNames, columns);
        }
    }

    @Override
    public RecordCursor getRecords(PartitionChunk partitionChunk)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getRecords(partitionChunk);
        }
    }

    @Override
    public byte[] serializePartitionChunk(PartitionChunk partitionChunk)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.serializePartitionChunk(partitionChunk);
        }
    }

    @Override
    public PartitionChunk deserializePartitionChunk(byte[] bytes)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.deserializePartitionChunk(bytes);
        }
    }
}
