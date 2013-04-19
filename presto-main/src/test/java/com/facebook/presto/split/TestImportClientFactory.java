package com.facebook.presto.split;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ImportClientFactory;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.SchemaTableMetadata;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
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
                "apple", new MockImportClientFactor(),
                "banana", new MockImportClientFactor()
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
        @Override
        public ImportClient createClient(String clientId)
        {
            return new ImportClient()
            {
                @Override
                public String getConnectorId()
                {
                    throw new UnsupportedOperationException();
                }

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
                public TableHandle createTable(SchemaTableMetadata tableMetadata)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void dropTable(TableHandle tableHandle)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Iterable<Split> getPartitionSplits(List<Partition> partitions)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public RecordSet getRecordSet(Split split, List<? extends ColumnHandle> columns)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean canHandle(TableHandle tableHandle)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean canHandle(ColumnHandle tableHandle)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean canHandle(Split split)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Class<? extends TableHandle> getTableHandleClass()
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Class<? extends ColumnHandle> getColumnHandleClass()
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Class<? extends Split> getSplitClass()
                {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
