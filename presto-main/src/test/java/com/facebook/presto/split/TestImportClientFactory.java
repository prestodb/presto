package com.facebook.presto.split;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ImportClientFactory;
import com.facebook.presto.spi.ObjectNotFoundException;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.spi.PartitionInfo;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaField;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestImportClientFactory
{
    @Test
    public void testGetClient()
            throws Exception
    {
        ImportClientManager factory = new ImportClientManager(ImmutableSet.<ImportClientFactory>of(new MockImportClientFactor("apple"), new MockImportClientFactor("banana")));
        assertInstanceOf(factory.getClient("apple"), ImportClient.class);
        assertInstanceOf(factory.getClient("banana"), ImportClient.class);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unknown source 'unknown'")
    public void testGetClientFailure()
    {
        new ImportClientManager(ImmutableSet.<ImportClientFactory>of()).getClient("unknown");
    }

    private class MockImportClientFactor
            implements ImportClientFactory
    {
        private final String sourceName;

        private MockImportClientFactor(String sourceName)
        {
            this.sourceName = sourceName;
        }

        @Override
        public ImportClient createClient(String sourceName)
        {
            if (!this.sourceName.equals(sourceName)) {
                return null;
            }

            return new ImportClient()
            {
                @Override
                public List<String> getDatabaseNames()
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<String> getTableNames(String databaseName)
                        throws ObjectNotFoundException
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<SchemaField> getTableSchema(String databaseName, String tableName)
                        throws ObjectNotFoundException
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<SchemaField> getPartitionKeys(String databaseName, String tableName)
                        throws ObjectNotFoundException
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<PartitionInfo> getPartitions(String databaseName, String tableName)
                        throws ObjectNotFoundException
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<PartitionInfo> getPartitions(String databaseName, String tableName, Map<String, Object> filters)
                        throws ObjectNotFoundException
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<String> getPartitionNames(String databaseName, String tableName)
                        throws ObjectNotFoundException
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<PartitionChunk> getPartitionChunks(String databaseName, String tableName, String partitionName, List<String> columns)
                        throws ObjectNotFoundException
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Iterable<List<PartitionChunk>> getPartitionChunks(String databaseName, String tableName, List<String> partitionNames, List<String> columns)
                        throws ObjectNotFoundException
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public RecordCursor getRecords(PartitionChunk partitionChunk)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public byte[] serializePartitionChunk(PartitionChunk partitionChunk)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public PartitionChunk deserializePartitionChunk(byte[] bytes)
                {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
