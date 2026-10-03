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
package com.facebook.presto.ducklake;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.ducklake.catalog.jdbc.JdbcDuckLakeCatalog;
import com.facebook.presto.ducklake.statistics.TableStatisticsMaker;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorTableVersion;
import com.facebook.presto.spi.connector.ConnectorTableVersion.VersionOperator;
import com.facebook.presto.spi.connector.ConnectorTableVersion.VersionType;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_SNAPSHOT;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_TYPE;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestDuckLakeMetadata
{
    private static final ConnectorSession SESSION = TestingConnectorSession.SESSION;

    private TestingDuckLakeCatalog testingCatalog;
    private JdbcDuckLakeCatalog catalog;
    private TypeManager typeManager;
    private DuckLakeMetadataFactory metadataFactory;

    @BeforeClass
    public void setUp()
    {
        testingCatalog = new TestingDuckLakeCatalog();
        catalog = testingCatalog.createCatalog();
        typeManager = FunctionAndTypeManager.createTestFunctionAndTypeManager();
        metadataFactory = new DuckLakeMetadataFactory(catalog, typeManager, new TableStatisticsMaker(catalog));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (testingCatalog != null) {
            testingCatalog.close();
        }
    }

    private DuckLakeMetadata newMetadata()
    {
        return metadataFactory.create();
    }

    @Test
    public void testListSchemaNames()
    {
        List<String> schemaNames = newMetadata().listSchemaNames(SESSION);
        assertTrue(schemaNames.containsAll(List.of("del", "evo", "inl", "main", "merge", "part", "tpch", "types")));
    }

    @Test
    public void testListTablesInTpch()
    {
        List<SchemaTableName> tables = newMetadata().listTables(SESSION, Optional.of("tpch"));
        Set<String> tableNames = tables.stream().map(SchemaTableName::getTableName).collect(Collectors.toSet());
        assertEquals(tableNames, ImmutableSet.of("customer", "nation", "orders", "region"));
        for (SchemaTableName table : tables) {
            assertEquals(table.getSchemaName(), "tpch");
        }
    }

    @Test
    public void testListAllTables()
    {
        List<SchemaTableName> tables = newMetadata().listTables(SESSION, Optional.empty());
        assertTrue(tables.contains(new SchemaTableName("types", "nested")));
        assertTrue(tables.contains(new SchemaTableName("merge", "table")));
    }

    @Test
    public void testGetTableHandleForNested()
    {
        DuckLakeMetadata metadata = newMetadata();
        DuckLakeTableHandle handle = (DuckLakeTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName("types", "nested"));
        assertNotNull(handle);
        assertFalse(handle.getTableName().isSnapshotSpecified());
        assertEquals(handle.getSnapshotId(), catalog.getLatestSnapshotId());

        List<ColumnMetadata> columns = metadata.getTableMetadata(SESSION, handle).getColumns();
        List<String> allNames = columns.stream().map(ColumnMetadata::getName).collect(Collectors.toList());
        assertEquals(allNames, List.of("id", "list_col", "struct_col", "map_col", "struct_with_list", "$path", "$row_id", "$row_position"));
        assertTrue(columns.get(columns.size() - 3).isHidden());
        assertTrue(columns.get(columns.size() - 2).isHidden());
        assertTrue(columns.get(columns.size() - 1).isHidden());

        Map<String, Type> typesByName = new LinkedHashMap<>();
        for (ColumnMetadata column : columns) {
            typesByName.put(column.getName(), column.getType());
        }
        assertEquals(typesByName.get("id"), INTEGER);
        assertEquals(typesByName.get("list_col"), new ArrayType(INTEGER));
        assertEquals(typesByName.get("struct_col"), RowType.from(ImmutableList.of(RowType.field("a", INTEGER), RowType.field("b", VARCHAR))));
        assertEquals(typesByName.get("map_col"), mapType(VARCHAR, INTEGER));
        assertEquals(typesByName.get("struct_with_list"), RowType.from(ImmutableList.of(RowType.field("name", VARCHAR), RowType.field("tags", new ArrayType(VARCHAR)))));

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, handle);
        assertEquals(columnHandles.keySet(), ImmutableSet.of("id", "list_col", "struct_col", "map_col", "struct_with_list", "$path", "$row_id", "$row_position"));
        DuckLakeColumnHandle structColumnHandle = (DuckLakeColumnHandle) columnHandles.get("struct_col");
        assertEquals(structColumnHandle.getType(), RowType.from(ImmutableList.of(RowType.field("a", INTEGER), RowType.field("b", VARCHAR))));
    }

    private Type mapType(Type key, Type value)
    {
        return typeManager.getParameterizedType(
                StandardTypes.MAP,
                ImmutableList.of(TypeSignatureParameter.of(key.getTypeSignature()), TypeSignatureParameter.of(value.getTypeSignature())));
    }

    @Test
    public void testGetTableHandleMissingSchemaOrTableIsNull()
    {
        DuckLakeMetadata metadata = newMetadata();
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("nosuch", "t")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("tpch", "nosuch")));
    }

    @Test
    public void testGetTableHandleSnapshotsSuffixIsNull()
    {
        DuckLakeMetadata metadata = newMetadata();
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("tpch", "orders$snapshots")));
    }

    @Test
    public void testGetTableHandleUnknownSuffixThrows()
    {
        DuckLakeMetadata metadata = newMetadata();
        PrestoException exception = expectThrows(PrestoException.class,
                () -> metadata.getTableHandle(SESSION, new SchemaTableName("tpch", "orders$files")));
        assertEquals(exception.getErrorCode(), NOT_SUPPORTED.toErrorCode());
    }

    @Test
    public void testUnsupportedTypeFailsTableMetadata()
    {
        DuckLakeMetadata metadata = newMetadata();
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("types", "unsupported"));
        assertNotNull(handle);

        PrestoException exception = expectThrows(PrestoException.class, () -> metadata.getTableMetadata(SESSION, handle));
        assertEquals(exception.getErrorCode(), DUCKLAKE_UNSUPPORTED_TYPE.toErrorCode());
        assertTrue(exception.getMessage().contains("interval_col"));
    }

    @Test
    public void testListTableColumnsSkipsUnsupportedTable()
    {
        DuckLakeMetadata metadata = newMetadata();
        Map<SchemaTableName, List<ColumnMetadata>> columns = metadata.listTableColumns(SESSION, new SchemaTablePrefix("types"));
        assertTrue(columns.containsKey(new SchemaTableName("types", "primitives")));
        assertTrue(columns.containsKey(new SchemaTableName("types", "nested")));
        assertFalse(columns.containsKey(new SchemaTableName("types", "unsupported")));
    }

    @Test
    public void testVersionAsOfEvoTableCreation()
            throws SQLException
    {
        DuckLakeMetadata metadata = newMetadata();
        long beginSnapshot = getBeginSnapshot(getTableId("evo", "table"));

        ConnectorTableVersion tableVersion = new ConnectorTableVersion(VersionType.VERSION, VersionOperator.EQUAL, BIGINT, beginSnapshot);
        DuckLakeTableHandle handle = (DuckLakeTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName("evo", "table"), Optional.of(tableVersion));
        assertNotNull(handle);
        assertTrue(handle.getTableName().isSnapshotSpecified());
        assertEquals(handle.getSnapshotId(), beginSnapshot);

        List<String> columnNames = metadata.getTableMetadata(SESSION, handle).getColumns().stream()
                .filter(column -> !column.isHidden())
                .map(ColumnMetadata::getName)
                .collect(Collectors.toList());
        assertEquals(columnNames, List.of("id", "name"));
    }

    @Test
    public void testVersionAsOfBadIdThrows()
    {
        DuckLakeMetadata metadata = newMetadata();
        ConnectorTableVersion tableVersion = new ConnectorTableVersion(VersionType.VERSION, VersionOperator.EQUAL, BIGINT, 999999L);
        PrestoException exception = expectThrows(PrestoException.class,
                () -> metadata.getTableHandle(SESSION, new SchemaTableName("evo", "table"), Optional.of(tableVersion)));
        assertEquals(exception.getErrorCode(), DUCKLAKE_INVALID_SNAPSHOT.toErrorCode());
    }

    @Test
    public void testVersionAsOfTableNotYetCreatedThrows()
    {
        DuckLakeMetadata metadata = newMetadata();
        // Snapshot 2 (the TPC-H load) is itself a perfectly valid snapshot, but schema "types"
        // (begin_snapshot 3) and its "primitives" table (begin_snapshot 4) do not exist yet at
        // that point. The engine's own MetadataUtil.getOptionalTableHandle() treats a null
        // connector result the same as no version at all and silently retries unversioned, so
        // this case must throw rather than return null (see getTableHandle's javadoc).
        ConnectorTableVersion tableVersion = new ConnectorTableVersion(VersionType.VERSION, VersionOperator.EQUAL, BIGINT, 2L);
        SchemaTableName tableName = new SchemaTableName("types", "primitives");
        TableNotFoundException exception = expectThrows(TableNotFoundException.class,
                () -> metadata.getTableHandle(SESSION, tableName, Optional.of(tableVersion)));
        assertEquals(exception.getMessage(), "Table types.primitives does not exist at DuckLake snapshot 2");

        // The unversioned overload must be unaffected: it keeps returning a real handle for a
        // table that does exist (at the latest snapshot), and null for one that plain doesn't.
        assertNotNull(metadata.getTableHandle(SESSION, tableName));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("nosuch", "primitives")));
    }

    @Test
    public void testTimestampAsOfLatestSnapshot()
    {
        DuckLakeMetadata metadata = newMetadata();
        long latest = catalog.getLatestSnapshotId();
        // The catalog stores microsecond-precision snapshot times but TIMESTAMP WITH TIME ZONE
        // (like a real SQL literal) only carries millisecond precision, so round up by a
        // millisecond to land at or after the latest snapshot's exact time; nothing exists after
        // it, so the resolved snapshot is unaffected.
        long millis = catalog.getSnapshot(latest).orElseThrow(AssertionError::new).getSnapshotTime().toEpochMilli() + 1;
        long packed = packDateTimeWithZone(millis, UTC_KEY);

        ConnectorTableVersion tableVersion = new ConnectorTableVersion(VersionType.TIMESTAMP, VersionOperator.EQUAL, TIMESTAMP_WITH_TIME_ZONE, packed);
        DuckLakeTableHandle handle = (DuckLakeTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName("tpch", "orders"), Optional.of(tableVersion));
        assertNotNull(handle);
        assertEquals(handle.getSnapshotId(), latest);
    }

    @Test
    public void testTimestampBeforeFirstSnapshotThrows()
    {
        DuckLakeMetadata metadata = newMetadata();
        long millis = catalog.getSnapshot(0).orElseThrow(AssertionError::new).getSnapshotTime().minusSeconds(86400).toEpochMilli();
        long packed = packDateTimeWithZone(millis, UTC_KEY);

        ConnectorTableVersion tableVersion = new ConnectorTableVersion(VersionType.TIMESTAMP, VersionOperator.EQUAL, TIMESTAMP_WITH_TIME_ZONE, packed);
        PrestoException exception = expectThrows(PrestoException.class,
                () -> metadata.getTableHandle(SESSION, new SchemaTableName("tpch", "orders"), Optional.of(tableVersion)));
        assertEquals(exception.getErrorCode(), DUCKLAKE_INVALID_SNAPSHOT.toErrorCode());
    }

    @Test
    public void testTimestampBeforeResolvesToPriorSnapshot()
            throws SQLException
    {
        DuckLakeMetadata metadata = newMetadata();
        // Snapshot 29 adds evo.table's "score" column (begin_snapshot 27 <= 28 < 29), so BEFORE
        // snapshot 29's time must resolve to snapshot 28, at which evo.table already exists (it
        // was created at snapshot 27) but "score" is not yet a column.
        long scoreColumnSnapshot = getBeginSnapshotForColumn("evo", "table", "score");
        assertEquals(scoreColumnSnapshot, 29L);
        long millis = catalog.getSnapshot(scoreColumnSnapshot).orElseThrow(AssertionError::new).getSnapshotTime().toEpochMilli();
        long packed = packDateTimeWithZone(millis, UTC_KEY);

        ConnectorTableVersion tableVersion = new ConnectorTableVersion(VersionType.TIMESTAMP, VersionOperator.LESS_THAN, TIMESTAMP_WITH_TIME_ZONE, packed);
        DuckLakeTableHandle handle = (DuckLakeTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName("evo", "table"), Optional.of(tableVersion));
        assertNotNull(handle);
        assertEquals(handle.getSnapshotId(), scoreColumnSnapshot - 1);

        List<String> columnNames = metadata.getTableMetadata(SESSION, handle).getColumns().stream()
                .filter(column -> !column.isHidden())
                .map(ColumnMetadata::getName)
                .collect(Collectors.toList());
        assertEquals(columnNames, List.of("id", "name"));
    }

    @Test
    public void testGetTableLayoutsRecordsPredicateColumn()
    {
        DuckLakeMetadata metadata = newMetadata();
        DuckLakeTableHandle handle = (DuckLakeTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName("tpch", "orders"));
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, handle);
        ColumnHandle orderKeyHandle = columnHandles.get("o_orderkey");
        assertNotNull(orderKeyHandle);

        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(Collections.singletonMap(orderKeyHandle, Domain.singleValue(BIGINT, 1L)));
        Constraint<ColumnHandle> constraint = new Constraint<>(domain);

        List<ConnectorTableLayoutResult> layouts = metadata.getTableLayouts(SESSION, handle, constraint, Optional.empty());
        assertEquals(layouts.size(), 1);
        DuckLakeTableLayoutHandle layoutHandle = (DuckLakeTableLayoutHandle) layouts.get(0).getTableLayout().getHandle();
        assertEquals(layoutHandle.getPredicateColumns().keySet(), ImmutableSet.of("o_orderkey"));
        assertEquals(layoutHandle.getDomainPredicate(), domain);
    }

    @Test
    public void testSnapshotsSystemTable()
    {
        DuckLakeMetadata metadata = newMetadata();
        Optional<SystemTable> systemTable = metadata.getSystemTable(SESSION, new SchemaTableName("tpch", "orders$snapshots"));
        assertTrue(systemTable.isPresent());

        int snapshotCount = catalog.listSnapshots().size();
        RecordCursor cursor = systemTable.get().cursor(null, SESSION, TupleDomain.all());
        int rowCount = 0;
        Set<String> commitMessages = new HashSet<>();
        while (cursor.advanceNextPosition()) {
            rowCount++;
            assertFalse(cursor.isNull(1), "snapshot_time must not be null");
            if (!cursor.isNull(4)) {
                commitMessages.add(cursor.getSlice(4).toStringUtf8());
            }
        }
        assertEquals(rowCount, snapshotCount);
        assertTrue(commitMessages.contains("Load TPC-H tiny fixture (nation, region, customer, orders)"));
        assertTrue(commitMessages.contains("Insert 1000 rows for del.simple fixture"));
    }

    @Test
    public void testSnapshotsSystemTableMissingTableIsEmpty()
    {
        DuckLakeMetadata metadata = newMetadata();
        assertFalse(metadata.getSystemTable(SESSION, new SchemaTableName("tpch", "nosuch$snapshots")).isPresent());
    }

    @Test
    public void testSystemTableForDataTypeIsEmpty()
    {
        DuckLakeMetadata metadata = newMetadata();
        assertFalse(metadata.getSystemTable(SESSION, new SchemaTableName("tpch", "orders")).isPresent());
    }

    private long getTableId(String schemaName, String tableName)
    {
        long latest = catalog.getLatestSnapshotId();
        return catalog.getTable(latest, catalog.getSchema(latest, schemaName).orElseThrow(AssertionError::new), tableName)
                .orElseThrow(AssertionError::new)
                .getTableId();
    }

    private long getBeginSnapshotForColumn(String schemaName, String tableName, String columnName)
            throws SQLException
    {
        long tableId = getTableId(schemaName, tableName);
        try (Connection connection = testingCatalog.openConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT begin_snapshot FROM public.ducklake_column WHERE table_id = ? AND column_name = ?")) {
            statement.setLong(1, tableId);
            statement.setString(2, columnName);
            try (ResultSet resultSet = statement.executeQuery()) {
                assertTrue(resultSet.next());
                return resultSet.getLong("begin_snapshot");
            }
        }
    }

    private long getBeginSnapshot(long tableId)
            throws SQLException
    {
        try (Connection connection = testingCatalog.openConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT begin_snapshot FROM public.ducklake_table WHERE table_id = ?")) {
            statement.setLong(1, tableId);
            try (ResultSet resultSet = statement.executeQuery()) {
                assertTrue(resultSet.next());
                return resultSet.getLong("begin_snapshot");
            }
        }
    }
}
