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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.cassandra.util.Types.checkType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestCassandraConnector
{
    public static final String keyspaceName = "Presto_Database";
    public static final String tableName = "Presto_Test";
    protected static final String INVALID_DATABASE = "totally_invalid_database";
    private static final ConnectorSession SESSION = new ConnectorSession("user", UTC_KEY, ENGLISH, System.currentTimeMillis(), null);
    private static final Date date = new Date();
    private static final String tableDefinition =
            "CREATE TABLE presto_test (" +
                    " key text PRIMARY KEY, " +
                    " typeuuid uuid, " +
                    " typeinteger int, " +
                    " typelong bigint, " +
                    " typebytes blob, " +
                    " typetimestamp timestamp " +
                    ")";
    protected String database;
    protected SchemaTableName table;
    protected SchemaTableName tableUnpartitioned;
    protected SchemaTableName invalidTable;
    private ConnectorMetadata metadata;
    private ConnectorSplitManager splitManager;
    private ConnectorRecordSetProvider recordSetProvider;

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
            String name = checkType(columnHandle, CassandraColumnHandle.class, "columnHandle").getName();
            index.put(name, i);
            i++;
        }
        return index.build();
    }

    public static void createTestData(Date date)
    {
        Cluster cluster = CassandraTestingUtils.getCluster();

        Session session = cluster.connect();

        CassandraTestingUtils.createOrReplaceKeyspace(session, keyspaceName);

        session.close();

        session = cluster.connect(keyspaceName);

        CassandraTestingUtils.createOrReplaceTable(session, tableName, tableDefinition);

        CassandraTestingUtils.createTestData(session, date);

        session.close();
    }

    @BeforeClass
    public void setup()
            throws Exception
    {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();

        createTestData(date);

        String connectorId = "cassandra-test";
        CassandraConnectorFactory connectorFactory = new CassandraConnectorFactory(
                connectorId,
                ImmutableMap.<String, String>of());

        Connector connector = connectorFactory.create(connectorId, ImmutableMap.of(
                "cassandra.contact-points" , CassandraTestingUtils.HOSTNAME ,
                "cassandra.native-protocol-port" , "" + CassandraTestingUtils.PORT));

        metadata = connector.getMetadata();
        assertInstanceOf(metadata, CassandraMetadata.class);

        splitManager = connector.getSplitManager();
        assertInstanceOf(splitManager, CassandraSplitManager.class);

        recordSetProvider = connector.getRecordSetProvider();
        assertInstanceOf(recordSetProvider, CassandraRecordSetProvider.class);

        ConnectorHandleResolver handleResolver = connector.getHandleResolver();
        assertInstanceOf(handleResolver, CassandraHandleResolver.class);

        database = keyspaceName.toLowerCase();
        table = new SchemaTableName(database, tableName.toLowerCase());
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
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("totally_invalid_database_name" , "dual")));
        assertEquals(metadata.listTables(SESSION, "totally_invalid_database_name"), ImmutableList.of());
        assertEquals(metadata.listTableColumns(SESSION, new SchemaTablePrefix("totally_invalid_database_name" , "dual")), ImmutableMap.of());
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

                    assertEquals(keyValue, String.format("key %d" , rowId));

                    // bytes are encoded as a hex string for some reason
                    // this check keeps failing for some reason; disabling it for now
                    assertEquals(cursor.getSlice(columnIndex.get("typebytes")).toStringUtf8(), String.format("0x%08X", rowId));

                    // VARINT is returned as a string
                    assertEquals(cursor.getSlice(columnIndex.get("typeinteger")).toStringUtf8(), String.valueOf(rowId));

                    assertEquals(cursor.getLong(columnIndex.get("typelong")), 1000 + rowId);

                    assertEquals(cursor.getSlice(columnIndex.get("typeuuid")).toStringUtf8(), String.format("00000000-0000-0000-0000-%012d" , rowId));

                    assertEquals(cursor.getSlice(columnIndex.get("typetimestamp")).toStringUtf8(), Long.valueOf(date.getTime()).toString());

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

    private ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, tableName);
        checkArgument(handle != null, "table not found: %s" , tableName);
        return handle;
    }

    @Table(keyspace = "presto_database" , name = "presto_test")
    public static class TableRow
    {
        @PartitionKey
        private String key;
        private UUID typeuuid;
        private Integer typeinteger;
        private Long typelong;
        private ByteBuffer typebytes;
        private Date typetimestamp;

        public TableRow() {}

        public TableRow(String key, UUID typeuuid, Integer typeinteger, Long typelong, ByteBuffer typebytes, Date typetimestamp)
        {
            this.key = key;
            this.typeuuid = typeuuid;
            this.typeinteger = typeinteger;
            this.typelong = typelong;
            this.typebytes = typebytes;
            this.typetimestamp = typetimestamp;
        }

        public String getKey()
        {
            return key;
        }

        public void setKey(String key)
        {
            this.key = key;
        }

        public UUID getTypeuuid()
        {
            return typeuuid;
        }

        public void setTypeuuid(UUID typeuuid)
        {
            this.typeuuid = typeuuid;
        }

        public Integer getTypeinteger()
        {
            return typeinteger;
        }

        public void setTypeinteger(Integer typeinteger)
        {
            this.typeinteger = typeinteger;
        }

        public Long getTypelong()
        {
            return typelong;
        }

        public void setTypelong(Long typelong)
        {
            this.typelong = typelong;
        }

        public ByteBuffer getTypebytes()
        {
            return typebytes;
        }

        public void setTypebytes(ByteBuffer typebytes)
        {
            this.typebytes = typebytes;
        }

        public Date getTypetimestamp()
        {
            return typetimestamp;
        }

        public void setTypetimestamp(Date typetimestamp)
        {
            this.typetimestamp = typetimestamp;
        }

        @Override
        public String toString()
        {
            return "TableRow{" +
                    "key='" + key + '\'' +
                    ", typeuuid=" + typeuuid +
                    ", typeinteger=" + typeinteger +
                    ", typelong=" + typelong +
                    ", typebytes=" + Charset.forName("UTF-8").decode(typebytes) +
                    ", typetimestamp=" + typetimestamp +
                    '}';
        }
    }
}
