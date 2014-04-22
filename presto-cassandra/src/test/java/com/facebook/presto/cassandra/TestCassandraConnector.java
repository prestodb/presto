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

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.cassandraunit.model.StrategyModel;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.spi.Session.DEFAULT_CATALOG;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestCassandraConnector
{
    private static final Session SESSION = new Session("user", "test", DEFAULT_CATALOG, "test", UTC_KEY, Locale.ENGLISH, null, null);
    protected static final String INVALID_DATABASE = "totally_invalid_database";

    private ConnectorMetadata metadata;
    private ConnectorSplitManager splitManager;
    private ConnectorRecordSetProvider recordSetProvider;

    protected String database;
    protected SchemaTableName table;
    protected SchemaTableName tableUnpartitioned;
    protected SchemaTableName invalidTable;

    @BeforeClass
    public void setup()
            throws Exception
    {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();

        createTestData();

        String connectorId = "cassandra-test";
        CassandraConnectorFactory connectorFactory = new CassandraConnectorFactory(
                connectorId,
                ImmutableMap.<String, String>of("node.environment", "test"));

        Connector connector = connectorFactory.create(connectorId, ImmutableMap.<String, String>of(
                "cassandra.contact-points", "localhost",
                "cassandra.native-protocol-port", "9142"));

        metadata = connector.getMetadata();
        assertInstanceOf(metadata, CassandraMetadata.class);

        splitManager = connector.getSplitManager();
        assertInstanceOf(splitManager, CassandraSplitManager.class);

        recordSetProvider = connector.getRecordSetProvider();
        assertInstanceOf(recordSetProvider, CassandraRecordSetProvider.class);

        ConnectorHandleResolver handleResolver = connector.getHandleResolver();
        assertInstanceOf(handleResolver, CassandraHandleResolver.class);

        database = "presto_database";
        table = new SchemaTableName(database, "presto_test");
        tableUnpartitioned = new SchemaTableName(database, "presto_test_unpartitioned");
        invalidTable = new SchemaTableName(database, "totally_invalid_table_name");
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        // todo how to stop cassandra
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
        assertTrue(databases.contains(database.toLowerCase()));
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
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(tableHandle);
        List<ConnectorColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        List<ConnectorSplit> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));

        long rowNumber = 0;
        for (ConnectorSplit split : splits) {
            CassandraSplit cassandraSplit = (CassandraSplit) split;

            long completedBytes = 0;
            try (RecordCursor cursor = recordSetProvider.getRecordSet(cassandraSplit, columnHandles).cursor()) {
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

                    assertEquals(keyValue, String.format("key %04d", rowId));
                    assertEquals(cursor.getSlice(columnIndex.get("t_utf8")).toStringUtf8(), "utf8 " + rowId);

                    // bytes are encoded as a hex string for some reason
                    assertEquals(cursor.getSlice(columnIndex.get("t_bytes")).toStringUtf8(), String.format("0x%08X", rowId));

                    // VARINT is returned as a string
                    assertEquals(cursor.getSlice(columnIndex.get("t_integer")).toStringUtf8(), String.valueOf(rowId));

                    assertEquals(cursor.getLong(columnIndex.get("t_long")), 1000 + rowId);

                    assertEquals(cursor.getSlice(columnIndex.get("t_uuid")).toStringUtf8(), String.format("00000000-0000-0000-0000-%012d", rowId));

                    // lexical UUIDs are encoded as a hex string for some reason
                    assertEquals(cursor.getSlice(columnIndex.get("t_lexical_uuid")).toStringUtf8(), String.format("0x%032X", rowId));

                    long newCompletedBytes = cursor.getCompletedBytes();
                    assertTrue(newCompletedBytes >= completedBytes);
                    completedBytes = newCompletedBytes;
                }
            }
        }
        assertEquals(rowNumber, 9);
    }

    private String toUtf8String(byte[] keys)
    {
        return new String(keys, Charsets.UTF_8);
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
                else if (BIGINT.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (DOUBLE.equals(type)) {
                    cursor.getDouble(columnIndex);
                }
                else if (VARCHAR.equals(type)) {
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
            List<ConnectorSplit> batch = splitSource.getNextBatch(1000);
            splits.addAll(batch);
        }
        return splits.build();
    }

    private static ImmutableMap<String, Integer> indexColumns(List<ConnectorColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ConnectorColumnHandle columnHandle : columnHandles) {
            checkArgument(columnHandle instanceof CassandraColumnHandle, "columnHandle is not an instance of CassandraColumnHandle");
            CassandraColumnHandle hiveColumnHandle = (CassandraColumnHandle) columnHandle;
            index.put(hiveColumnHandle.getName(), i);
            i++;
        }
        return index.build();
    }

    public static void createTestData()
    {
        String clusterName = "TestCluster";
        String host = "localhost:9171";

        Cluster cluster = HFactory.getOrCreateCluster(clusterName, host);
        Keyspace keyspace = HFactory.createKeyspace("beautifulKeyspaceName", cluster);
        assertNotNull(keyspace);

        String keyspaceName = "Presto_Database";
        String columnFamilyName = "Presto_Test";
        List<ColumnFamilyDefinition> columnFamilyDefinitions = createColumnFamilyDefinitions(keyspaceName, columnFamilyName);
        KeyspaceDefinition keyspaceDefinition = HFactory.createKeyspaceDefinition(
                keyspaceName,
                StrategyModel.SIMPLE_STRATEGY.value(),
                1,
                columnFamilyDefinitions);

        if (cluster.describeKeyspace(keyspaceName) != null) {
            cluster.dropKeyspace(keyspaceName, true);
        }
        cluster.addKeyspace(keyspaceDefinition, true);
        keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

        long timestamp = System.currentTimeMillis();
        for (int rowNumber = 1; rowNumber < 10; rowNumber++) {
            addRow(columnFamilyName, mutator, timestamp, rowNumber);
        }
        mutator.execute();
    }

    private static void addRow(String columnFamilyName, Mutator<String> mutator, long timestamp, int rowNumber)
    {
        String key = String.format("key %04d", rowNumber);
        mutator.addInsertion(
                key,
                columnFamilyName,
                HFactory.createColumn(
                        "t_utf8",
                        "utf8 " + rowNumber,
                        timestamp,
                        StringSerializer.get(),
                        StringSerializer.get()));
        mutator.addInsertion(
                key,
                columnFamilyName,
                HFactory.createColumn(
                        "t_bytes",
                        Ints.toByteArray(rowNumber),
                        timestamp,
                        StringSerializer.get(),
                        BytesArraySerializer.get()));
        mutator.addInsertion(
                key,
                columnFamilyName,
                HFactory.createColumn(
                        "t_integer",
                        rowNumber,
                        timestamp,
                        StringSerializer.get(),
                        IntegerSerializer.get()));
        mutator.addInsertion(
                key,
                columnFamilyName,
                HFactory.createColumn(
                        "t_long",
                        1000L + rowNumber,
                        timestamp,
                        StringSerializer.get(),
                        LongSerializer.get()));
        mutator.addInsertion(
                key,
                columnFamilyName,
                HFactory.createColumn(
                        "t_uuid",
                        UUID.fromString(String.format("00000000-0000-0000-0000-%012d", rowNumber)),
                        timestamp,
                        StringSerializer.get(),
                        UUIDSerializer.get()));
        mutator.addInsertion(
                key,
                columnFamilyName,
                HFactory.createColumn(
                        "t_lexical_uuid",
                        UUID.fromString(String.format("00000000-0000-0000-0000-%012d", rowNumber)),
                        timestamp,
                        StringSerializer.get(),
                        UUIDSerializer.get()));
    }

    private static List<ColumnFamilyDefinition> createColumnFamilyDefinitions(String keyspaceName, String columnFamilyName)
    {
        List<ColumnFamilyDefinition> columnFamilyDefinitions = new ArrayList<>();

        ImmutableList.Builder<ColumnDefinition> columnsDefinition = ImmutableList.builder();

        columnsDefinition.add(createColumnDefinition("t_utf8", ComparatorType.UTF8TYPE));
        columnsDefinition.add(createColumnDefinition("t_bytes", ComparatorType.BYTESTYPE));
        columnsDefinition.add(createColumnDefinition("t_integer", ComparatorType.INTEGERTYPE));
        columnsDefinition.add(createColumnDefinition("t_int32", ComparatorType.INT32TYPE));
        columnsDefinition.add(createColumnDefinition("t_long", ComparatorType.LONGTYPE));
        columnsDefinition.add(createColumnDefinition("t_boolean", ComparatorType.BOOLEANTYPE));
        columnsDefinition.add(createColumnDefinition("t_uuid", ComparatorType.UUIDTYPE));
        columnsDefinition.add(createColumnDefinition("t_lexical_uuid", ComparatorType.LEXICALUUIDTYPE));

        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(
                keyspaceName,
                columnFamilyName,
                ComparatorType.UTF8TYPE,
                columnsDefinition.build());

        cfDef.setColumnType(ColumnType.STANDARD);
        cfDef.setComment("presto test table");

        cfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getTypeName());

        columnFamilyDefinitions.add(cfDef);

        return columnFamilyDefinitions;
    }

    private static BasicColumnDefinition createColumnDefinition(String columnName, ComparatorType type)
    {
        BasicColumnDefinition columnDefinition = new BasicColumnDefinition();
        columnDefinition.setName(ByteBuffer.wrap(columnName.getBytes(Charsets.UTF_8)));
        columnDefinition.setValidationClass(type.getClassName());
        return columnDefinition;
    }
}
