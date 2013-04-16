package com.facebook.presto.split;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ImportClientFactory;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
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
        ImportClientManager factory = new ImportClientManager(ImmutableMap.<String, ImportClientFactory>of(
                "apple", new MockImportClientFactor("apple"),
                "banana", new MockImportClientFactor("banana")
        ));
        assertInstanceOf(factory.getClient("apple"), ImportClient.class);
        assertInstanceOf(factory.getClient("banana"), ImportClient.class);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unknown source 'unknown'")
    public void testGetClientFailure()
    {
        new ImportClientManager(ImmutableMap.<String, ImportClientFactory>of()).getClient("unknown");
    }

    private class MockImportClientFactor
            implements ImportClientFactory
    {
        private final String catalogName;

        private MockImportClientFactor(String catalogName)
        {
            Preconditions.checkNotNull(catalogName, "catalogName is null");
            this.catalogName = catalogName;
        }

        @Override
        public boolean hasCatalog(String catalogName)
        {
            return this.catalogName.equals(catalogName);
        }

        @Override
        public ImportClient createClient(String catalogName)
        {
            if (!this.catalogName.equals(catalogName)) {
                return null;
            }

            return new ImportClient()
            {
                @Override
                public List<String> listSchemaNames()
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public TableHandle getTableHandle(SchemaTableName tableName)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public SchemaTableName getTableName(TableHandle tableHandle)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public SchemaTableMetadata getTableMetadata(TableHandle table)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<SchemaTableName> listTables(String schemaNameOrNull)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Iterable<PartitionChunk> getPartitionChunks(List<Partition> partitions, List<ColumnHandle> columns)
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
