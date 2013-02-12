/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.thrift;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ObjectNotFoundException;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.spi.PartitionInfo;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.thrift.spi.PrestoImporter;
import com.facebook.presto.thrift.spi.PrestoImporterException;
import com.facebook.presto.thrift.spi.PrestoPartitionChunk;
import com.facebook.presto.thrift.spi.PrestoPartitionInfo;
import com.facebook.presto.thrift.spi.PrestoSchemaField;
import com.facebook.swift.service.ThriftClient;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Functions.toStringFunction;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

public class PrestoThriftImportClient
        implements ImportClient
{
    private static final Function<PrestoSchemaField, SchemaField> TRANSLATE_FIELD = new Function<PrestoSchemaField, SchemaField>()
    {
        @Override
        public SchemaField apply(PrestoSchemaField field)
        {
            SchemaField.Type type;

            switch (field.getType()) {
                case DOUBLE:
                    type = SchemaField.Type.DOUBLE;
                    break;
                case LONG:
                    type = SchemaField.Type.LONG;
                    break;
                case STRING:
                    type = SchemaField.Type.STRING;
                    break;
                default:
                    throw new IllegalStateException("Unknown type: " + field.getType());
            }

            return SchemaField.createPrimitive(field.getName(), field.getId(), type);
        }
    };
    private static final Predicate<PrestoSchemaField> IS_PARTITION_KEY = new Predicate<PrestoSchemaField>()
    {
        @Override
        public boolean apply(PrestoSchemaField field)
        {
            return field.isPartitionKey();
        }
    };

    private final ThriftClient<PrestoImporter> client;
    private final HostAndPort serviceAddress;

    public PrestoThriftImportClient(ThriftClient<PrestoImporter> client, HostAndPort serviceAddress)
    {
        this.client = client;
        this.serviceAddress = serviceAddress;
    }

    @Override
    public List<String> getDatabaseNames()
    {
        try (PrestoImporter importer = open()) {
            return importer.getDatabaseNames();
        }
        catch (PrestoImporterException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<String> getTableNames(String databaseName) throws ObjectNotFoundException
    {
        try (PrestoImporter importer = open()) {
            List<String> databaseNames = importer.getTableNames(databaseName);

            if (databaseNames == null) {
                throw new ObjectNotFoundException("Failed to find " + databaseName);
            }

            return databaseNames;
        }
        catch (PrestoImporterException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<SchemaField> getTableSchema(String databaseName, String tableName) throws ObjectNotFoundException
    {
        return Lists.transform(loadSchema(databaseName, tableName), TRANSLATE_FIELD);
    }

    @Override
    public List<SchemaField> getPartitionKeys(String databaseName, String tableName) throws ObjectNotFoundException
    {
        return ImmutableList.copyOf(transform(filter(loadSchema(databaseName, tableName), IS_PARTITION_KEY), TRANSLATE_FIELD));
    }

    @Override
    public List<PartitionInfo> getPartitions(String databaseName, String tableName) throws ObjectNotFoundException
    {
        return getPartitions(databaseName, tableName, ImmutableMap.<String, Object>of());
    }

    @Override
    public List<PartitionInfo> getPartitions(String databaseName, String tableName, Map<String, Object> filters) throws ObjectNotFoundException
    {
        try (PrestoImporter importer = open()) {
            List<PrestoPartitionInfo> partitions = importer.getPartitions(databaseName, tableName, Maps.transformValues(filters, toStringFunction()));
            ImmutableList.Builder<PartitionInfo> result = ImmutableList.builder();

            if (partitions == null) {
                throw new ObjectNotFoundException("Failed to find " + databaseName + " " + tableName);
            }

            for (PrestoPartitionInfo partition : partitions) {
                result.add(new PartitionInfo(partition.getName(), partition.getKeys()));
            }

            return result.build();
        }
        catch (PrestoImporterException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<String> getPartitionNames(String databaseName, String tableName) throws ObjectNotFoundException
    {
        List<PartitionInfo> partitions = getPartitions(databaseName, tableName);
        ImmutableList.Builder<String> result = ImmutableList.builder();

        for (PartitionInfo partition : partitions) {
            result.add(partition.getName());
        }

        return result.build();
    }

    @Override
    public Iterable<PartitionChunk> getPartitionChunks(String databaseName, String tableName, String partitionName, List<String> columns) throws ObjectNotFoundException
    {
        return getPartitionChunks(databaseName, tableName, ImmutableList.of(partitionName), columns);
    }

    @Override
    public Iterable<PartitionChunk> getPartitionChunks(String databaseName, String tableName, List<String> partitionNames, List<String> columns) throws ObjectNotFoundException
    {
        try (PrestoImporter importer = open()) {
            List<PrestoPartitionChunk> chunks = importer.getPartitionChunks(databaseName, tableName, partitionNames, columns);
            ImmutableList.Builder<PartitionChunk> result = ImmutableList.builder();

            if (chunks == null) {
                throw new ObjectNotFoundException("Failed to find " + databaseName + " " + tableName);
            }

            for (PrestoPartitionChunk chunk : chunks) {
                result.add(new PrestoThriftPartitionChunk(chunk));
            }

            return result.build();
        }
        catch (PrestoImporterException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public RecordCursor getRecords(PartitionChunk partitionChunk)
    {
        try (PrestoImporter importer = open()) {
            List<Map<Integer, String>> records = importer.getRecords((PrestoPartitionChunk) partitionChunk);

            return new PrestoThriftRecordCursor(records);
        }
        catch (PrestoImporterException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    @SuppressWarnings("NumericCastThatLosesPrecision")
    public byte[] serializePartitionChunk(PartitionChunk partitionChunk)
    {
        PrestoThriftPartitionChunk thriftChunk = (PrestoThriftPartitionChunk) partitionChunk;
        long length = thriftChunk.getLength();
        byte[] clientData = thriftChunk.getClientData();
        byte[] result = new byte[clientData.length + 8];

        result[0] = (byte) ((length >> 56) & 0xFF);
        result[1] = (byte) ((length >> 48) & 0xFF);
        result[2] = (byte) ((length >> 40) & 0xFF);
        result[3] = (byte) ((length >> 32) & 0xFF);
        result[4] = (byte) ((length >> 24) & 0xFF);
        result[5] = (byte) ((length >> 16) & 0xFF);
        result[6] = (byte) ((length >> 8) & 0xFF);
        result[7] = (byte) (length & 0xFF);
        System.arraycopy(clientData, 0, result, 8, clientData.length);

        return result;
    }

    @Override
    public PartitionChunk deserializePartitionChunk(byte[] bytes)
    {
        long length;
        byte[] clientData = new byte[bytes.length - 8];

        length = bytes[0] & 0xFF;
        length <<= 8;
        length = bytes[1] & 0xFF;
        length <<= 8;
        length = bytes[2] & 0xFF;
        length <<= 8;
        length = bytes[3] & 0xFF;
        length <<= 8;
        length = bytes[4] & 0xFF;
        length <<= 8;
        length = bytes[5] & 0xFF;
        length <<= 8;
        length = bytes[6] & 0xFF;
        length <<= 8;
        length = bytes[7] & 0xFF;
        System.arraycopy(bytes, 8, clientData, 0, clientData.length);

        return new PrestoThriftPartitionChunk(new PrestoPartitionChunk(length, clientData));
    }

    private PrestoImporter open()
    {
        try {
            return client.open(serviceAddress);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private List<PrestoSchemaField> loadSchema(String databaseName, String tableName) throws ObjectNotFoundException
    {
        try (PrestoImporter importer = open()) {
            List<PrestoSchemaField> schema = importer.getTableSchema(databaseName, tableName);

            if (schema == null) {
                throw new ObjectNotFoundException("Failed to find schema for " + databaseName + " " + tableName);
            }

            return schema;
        }
        catch (PrestoImporterException e) {
            throw Throwables.propagate(e);
        }
    }
}
