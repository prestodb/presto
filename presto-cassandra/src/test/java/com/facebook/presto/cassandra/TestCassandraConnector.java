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

// Driver 4.x: Bytes utility removed, use ByteBuffer directly
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
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
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingContext;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.testing.TestingConnectorContext;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_ALL_TYPES;
import static com.facebook.presto.cassandra.CassandraTestingUtils.createTestTables;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Locale.ROOT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestCassandraConnector
{
    protected static final String INVALID_DATABASE = "totally_invalid_database";
    private static final Date DATE = new Date();
    private static final ConnectorSession SESSION = new TestingConnectorSession(
            "user",
            new ConnectorIdentity("user", Optional.empty(), Optional.empty()),
            Optional.of("test"),
            Optional.empty(),
            UTC_KEY,
            ENGLISH,
            System.currentTimeMillis(),
            new CassandraSessionProperties(new CassandraClientConfig()).getSessionProperties(),
            ImmutableMap.of(),
            true,
            Optional.empty(),
            ImmutableSet.of(),
            Optional.empty(),
            ImmutableMap.of());
    protected String database;

    protected SchemaTableName table;
    protected SchemaTableName tableUnpartitioned;
    protected SchemaTableName invalidTable;
    protected SchemaTableName rollbackTable;
    private CassandraServer server;
    private ConnectorSplitManager splitManager;
    private ConnectorRecordSetProvider recordSetProvider;
    private Connector connector;

    @BeforeClass
    public void setup()
            throws Exception
    {
        this.server = new CassandraServer();

        String keyspace = "test_connector";
        createTestTables(server.getSession(), server.getMetadata(), keyspace, DATE);

        String connectorId = "cassandra-test";
        CassandraConnectorFactory connectorFactory = new CassandraConnectorFactory(connectorId);

        connector = connectorFactory.create(connectorId, ImmutableMap.of(
                        "cassandra.contact-points", server.getHost(),
                        "cassandra.native-protocol-port", Integer.toString(server.getPort()),
                        "cassandra.allow-drop-table", "true",
                        "cassandra.load-policy.use-dc-aware", "true",
                        "cassandra.load-policy.dc-aware.local-dc", "datacenter1",
                        "cassandra.consistency-level", "LOCAL_QUORUM"),
                new TestingConnectorContext());

        splitManager = connector.getSplitManager();
        assertInstanceOf(splitManager, CassandraSplitManager.class);

        recordSetProvider = connector.getRecordSetProvider();
        assertInstanceOf(recordSetProvider, CassandraRecordSetProvider.class);

        database = keyspace;
        table = new SchemaTableName(database, TABLE_ALL_TYPES.toLowerCase(ROOT));
        tableUnpartitioned = new SchemaTableName(database, "presto_test_unpartitioned");
        invalidTable = new SchemaTableName(database, "totally_invalid_table_name");
        rollbackTable = new SchemaTableName(database, "rollback_table");
    }

    @Test
    public void testGetClient()
    {
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        server.close();
    }

    @Test
    public void testGetDatabaseNames()
    {
        ConnectorTransactionHandle transactionHandle = connector.beginTransaction(READ_UNCOMMITTED, true);
        ConnectorMetadata metadata = connector.getMetadata(transactionHandle);
        List<String> databases = metadata.listSchemaNames(SESSION);
        assertTrue(databases.contains(database.toLowerCase(ROOT)));
    }

    @Test
    public void testGetTableNames()
    {
        ConnectorTransactionHandle transactionHandle = connector.beginTransaction(READ_UNCOMMITTED, true);
        ConnectorMetadata metadata = connector.getMetadata(transactionHandle);
        List<SchemaTableName> tables = metadata.listTables(SESSION, database);
        assertTrue(tables.contains(table));
    }

    // disabled until metadata manager is updated to handle invalid catalogs and schemas
    @Test(enabled = false, expectedExceptions = SchemaNotFoundException.class)
    public void testGetTableNamesException()
    {
        ConnectorTransactionHandle transactionHandle = connector.beginTransaction(READ_UNCOMMITTED, true);
        ConnectorMetadata metadata = connector.getMetadata(transactionHandle);
        metadata.listTables(SESSION, INVALID_DATABASE);
    }

    @Test
    public void testListUnknownSchema()
    {
        ConnectorTransactionHandle transactionHandle = connector.beginTransaction(READ_UNCOMMITTED, true);
        ConnectorMetadata metadata = connector.getMetadata(transactionHandle);
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("totally_invalid_database_name", "dual")));
        assertEquals(metadata.listTables(SESSION, "totally_invalid_database_name"), ImmutableList.of());
        assertEquals(metadata.listTableColumns(SESSION, new SchemaTablePrefix("totally_invalid_database_name", "dual")), ImmutableMap.of());
    }

    @Test
    public void testGetRecords()
    {
        ConnectorTransactionHandle transactionHandle = connector.beginTransaction(READ_UNCOMMITTED, true);
        ConnectorMetadata metadata = connector.getMetadata(transactionHandle);
        ConnectorTableHandle tableHandle = getTableHandle(table, metadata);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        ConnectorTableLayoutResult layoutResult = metadata.getTableLayoutForConstraint(SESSION, tableHandle, Constraint.alwaysTrue(), Optional.empty());
        ConnectorTableLayoutHandle layout = layoutResult.getTableLayout().getHandle();
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(transactionHandle, SESSION, layout, new SplitSchedulingContext(UNGROUPED_SCHEDULING, false, WarningCollector.NOOP)));

        long rowNumber = 0;
        for (ConnectorSplit split : splits) {
            CassandraSplit cassandraSplit = (CassandraSplit) split;

            long completedBytes = 0;
            try (RecordCursor cursor = recordSetProvider.getRecordSet(transactionHandle, SESSION, cassandraSplit, columnHandles).cursor()) {
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

                    // Convert bytes to hex string manually
                    byte[] bytes = cursor.getSlice(columnIndex.get("typebytes")).getBytes();
                    StringBuilder hex = new StringBuilder("0x");
                    for (byte b : bytes) {
                        hex.append(String.format("%02X", b));
                    }
                    assertEquals(hex.toString(), String.format("0x%08X", rowId));

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

    @Test
    public void testRollbackTables()
    {
        ConnectorTableMetadata connectorTableMetadata = new ConnectorTableMetadata(
                rollbackTable,
                ImmutableList.of(
                        ColumnMetadata.builder()
                                .setName("test_col")
                                .setType(BIGINT)
                                .build()));

        // start a transaction
        ConnectorTransactionHandle transactionHandle = connector.beginTransaction(READ_UNCOMMITTED, true);
        ConnectorMetadata metadata = connector.getMetadata(transactionHandle);
        ConnectorOutputTableHandle handle = null;

        try {
            // Begin table creation (STAGING only)
            handle = metadata.beginCreateTable(SESSION, connectorTableMetadata, Optional.empty());
            // simulate a failure
            throw new RuntimeException("Force failure before finish");
        }
        catch (RuntimeException e) {
            if (handle != null) {
                // table should exist
                assertTrue(metadata.listTables(SESSION, database).contains(rollbackTable));
                // rollback table
                connector.rollback(transactionHandle);
            }
        }
        assertFalse(metadata.listTables(SESSION, database).contains(rollbackTable));
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
                else if (REAL.equals(type)) {
                    cursor.getLong(columnIndex);
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

    private ConnectorTableHandle getTableHandle(SchemaTableName tableName, ConnectorMetadata metadata)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            splits.addAll(getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits());
        }
        return splits.build();
    }

    private static ImmutableMap<String, Integer> indexColumns(List<ColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            String name = ((CassandraColumnHandle) columnHandle).getName();
            index.put(name, i);
            i++;
        }
        return index.build();
    }
}
