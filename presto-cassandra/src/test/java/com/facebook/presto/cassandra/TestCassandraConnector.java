/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cassandra;

import com.datastax.driver.core.utils.Bytes;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.cassandra.CassandraTestingUtils.HOSTNAME;
import static com.facebook.presto.cassandra.CassandraTestingUtils.KEYSPACE_NAME;
import static com.facebook.presto.cassandra.CassandraTestingUtils.PORT;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_NAME;
import static com.facebook.presto.cassandra.CassandraTestingUtils.initializeTestData;
import static com.facebook.presto.cassandra.util.Types.checkType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestCassandraConnector
{
    protected static final String INVALID_DATABASE = "totally_invalid_database";
    private static final Date DATE = new Date();
    protected String database;
    protected SchemaTableName table;
    protected SchemaTableName tableUnpartitioned;
    protected SchemaTableName invalidTable;
    private ConnectorMetadata metadata;
    private ConnectorSplitManager splitManager;
    private ConnectorRecordSetProvider recordSetProvider;

    @BeforeClass
    public void setup()
            throws Exception
    {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();

        initializeTestData(DATE);

        String connectorId = "cassandra-test";
        CassandraConnectorFactory connectorFactory = new CassandraConnectorFactory(
                connectorId,
                ImmutableMap.<String, String>of());

        Connector connector = connectorFactory.create(connectorId, ImmutableMap.of(
                "cassandra.contact-points", HOSTNAME,
                "cassandra.native-protocol-port", Integer.toString(PORT)));

        metadata = connector.getMetadata(CassandraTransactionHandle.INSTANCE);
        assertInstanceOf(metadata, CassandraMetadata.class);

        splitManager = connector.getSplitManager();
        assertInstanceOf(splitManager, CassandraSplitManager.class);

        recordSetProvider = connector.getRecordSetProvider();
        assertInstanceOf(recordSetProvider, CassandraRecordSetProvider.class);

        database = KEYSPACE_NAME.toLowerCase();
        table = new SchemaTableName(database, TABLE_NAME.toLowerCase());
        tableUnpartitioned = new SchemaTableName(database, "presto_test_unpartitioned");
        invalidTable = new SchemaTableName(database, "totally_invalid_table_name");
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
    }

    @Test
    public void testGetClient()
    {
    }

    @Test
    public void testGetDatabaseNames()
            throws Exception
    {
        List<String> databases = metadata.listSchemaNames(SESSION);
        assertTrue(databases.contains(database.toLowerCase(ENGLISH)));
    }

    @Test
    public void testGetTableNames()
            throws Exception
    {
        List<SchemaTableName> tables = metadata.listTables(SESSION, database);
        assertTrue(tables.contains(table));
    }

    // disabled until metadata manager is updated to handle invalid catalogs and schemas
    @Test(enabled = false, expectedExceptions = SchemaNotFoundException.class)
    public void testGetTableNamesException()
            throws Exception
    {
        metadata.listTables(SESSION, INVALID_DATABASE);
    }

    @Test
    public void testListUnknownSchema()
    {
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("totally_invalid_database_name", "dual")));
        assertEquals(metadata.listTables(SESSION, "totally_invalid_database_name"), ImmutableList.of());
        assertEquals(metadata.listTableColumns(SESSION, new SchemaTablePrefix("totally_invalid_database_name", "dual")), ImmutableMap.of());
    }

    @Test
    public void testGetRecords()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(table);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        ConnectorTransactionHandle transaction = CassandraTransactionHandle.INSTANCE;

        List<ConnectorTableLayoutResult> layouts = metadata.getTableLayouts(SESSION, tableHandle, Constraint.alwaysTrue(), Optional.empty());
        ConnectorTableLayoutHandle layout = getOnlyElement(layouts).getTableLayout().getHandle();
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(transaction, SESSION, layout));

        long rowNumber = 0;
        for (ConnectorSplit split : splits) {
            CassandraSplit cassandraSplit = (CassandraSplit) split;

            long completedBytes = 0;
            try (RecordCursor cursor = recordSetProvider.getRecordSet(transaction, SESSION, cassandraSplit, columnHandles).cursor()) {
                while (cursor.advanceNextPosition()) {
                    try {
                        assertReadFields(cursor, tableMetadata.getColumns());
                    }
                    catch (RuntimeException e) {
                        throw new RuntimeException("row " + rowNumber, e);
                    }

                    rowNumber++;

                    String keyValue = cursor.getSlice(columnIndex.get("key")).toStringUtf8();
                    assertTrue(keyValue.startsWith("key "));
                    int rowId = Integer.parseInt(keyValue.substring(4));

                    assertEquals(keyValue, String.format("key %d", rowId));

                    assertEquals(Bytes.toHexString(cursor.getSlice(columnIndex.get("typebytes")).getBytes()), String.format("0x%08X", rowId));

                    // VARINT is returned as a string
                    assertEquals(cursor.getSlice(columnIndex.get("typeinteger")).toStringUtf8(), String.valueOf(rowId));

                    assertEquals(cursor.getLong(columnIndex.get("typelong")), 1000 + rowId);

                    assertEquals(cursor.getSlice(columnIndex.get("typeuuid")).toStringUtf8(), String.format("00000000-0000-0000-0000-%012d", rowId));

                    assertEquals(cursor.getSlice(columnIndex.get("typetimestamp")).toStringUtf8(), Long.valueOf(DATE.getTime()).toString());

                    long newCompletedBytes = cursor.getCompletedBytes();
                    assertTrue(newCompletedBytes >= completedBytes);
                    completedBytes = newCompletedBytes;
                }
            }
        }
        assertEquals(rowNumber, 9);
    }

    private static void assertReadFields(RecordCursor cursor, List<ColumnMetadata> schema)
    {
        for (int columnIndex = 0; columnIndex < schema.size(); columnIndex++) {
            ColumnMetadata column = schema.get(columnIndex);
            if (!cursor.isNull(columnIndex)) {
                Type type = column.getType();
                if (BOOLEAN.equals(type)) {
                    cursor.getBoolean(columnIndex);
                }
                else if (INTEGER.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (BIGINT.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (TIMESTAMP.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (DOUBLE.equals(type)) {
                    cursor.getDouble(columnIndex);
                }
                else if (isVarcharType(type) || VARBINARY.equals(type)) {
                    try {
                        cursor.getSlice(columnIndex);
                    }
                    catch (RuntimeException e) {
                        throw new RuntimeException("column " + column, e);
                    }
                }
                else {
                    fail("Unknown primitive type " + columnIndex);
                }
            }
        }
    }

    private ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
            throws InterruptedException
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            splits.addAll(getFutureValue(splitSource.getNextBatch(1000)));
        }
        return splits.build();
    }

    private static ImmutableMap<String, Integer> indexColumns(List<ColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            String name = checkType(columnHandle, CassandraColumnHandle.class, "columnHandle").getName();
            index.put(name, i);
            i++;
        }
        return index.build();
    }
}
