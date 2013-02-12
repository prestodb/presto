/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.thrift.spi;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

@ThriftService
public interface PrestoImporter extends Closeable
{
    @ThriftMethod
    public List<String> getDatabaseNames() throws PrestoImporterException;

    @ThriftMethod
    public List<String> getTableNames(String databaseName) throws PrestoImporterException;

    @ThriftMethod
    public List<PrestoSchemaField> getTableSchema(String databaseName, String tableName) throws PrestoImporterException;

    @ThriftMethod
    public List<PrestoPartitionInfo> getPartitions(String databaseName, String tableName, Map<String, String> filters) throws PrestoImporterException;

    @ThriftMethod
    public List<PrestoPartitionChunk> getPartitionChunks(String databaseName, String tableName, List<String> partitionNames, List<String> columns) throws PrestoImporterException;

    @ThriftMethod
    public List<Map<Integer, String>> getRecords(PrestoPartitionChunk partitionChunk) throws PrestoImporterException;

    @Override
    void close();
}
