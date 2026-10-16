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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.ducklake.catalog.DuckLakePartitionField;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Providers;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.ducklake.DuckLakeColumnIdentity.TypeCategory.ARRAY;
import static com.facebook.presto.ducklake.DuckLakeColumnIdentity.TypeCategory.MAP;
import static com.facebook.presto.ducklake.DuckLakeColumnIdentity.TypeCategory.PRIMITIVE;
import static com.facebook.presto.ducklake.DuckLakeColumnIdentity.TypeCategory.STRUCT;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDuckLakeHandles
{
    @Test
    public void testTableHandleJsonRoundTrip()
    {
        DuckLakeTableHandle expected = new DuckLakeTableHandle(
                "test_schema",
                DuckLakeTableName.from("orders").withSnapshotId(10, true),
                42L,
                7L,
                "/data/ducklake/orders",
                nestedSchema());

        JsonCodec<DuckLakeTableHandle> codec = JsonCodec.jsonCodec(DuckLakeTableHandle.class);
        String json = codec.toJson(expected);
        DuckLakeTableHandle actual = codec.fromJson(json);

        assertEquals(actual, expected);
        assertEquals(actual.getSchemaTableName(), expected.getSchemaTableName());
        assertEquals(actual.getSnapshotId(), 10L);
        assertEquals(actual.getSchema(), expected.getSchema());
    }

    @Test
    public void testTableHandleGetSnapshotIdThrowsWhenUnresolved()
    {
        DuckLakeTableHandle handle = new DuckLakeTableHandle(
                "test_schema", DuckLakeTableName.from("orders"), 1L, 7L, "/data/ducklake/orders", nestedSchema());
        try {
            handle.getSnapshotId();
            throw new AssertionError("expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
            // expected: from() never resolves a snapshot id
        }
    }

    @Test
    public void testRegularColumnHandleJsonRoundTrip()
    {
        DuckLakeColumnHandle expected = DuckLakeColumnHandle.primitiveColumnHandle(1, "id", "int32", INTEGER);
        assertRoundTrip(expected);
        assertFalse(expected.isPathColumn());
        assertFalse(expected.isRowIdColumn());
        assertFalse(expected.isRowPositionColumn());
    }

    @Test
    public void testColumnHandlesDifferingOnlyInDefaultValueAreNotEqual()
    {
        DuckLakeColumnIdentity identity = new DuckLakeColumnIdentity(1, "id", PRIMITIVE, "int32", ImmutableList.of());
        DuckLakeColumnHandle withoutDefault = new DuckLakeColumnHandle(identity, INTEGER, REGULAR, Optional.empty());
        DuckLakeColumnHandle withDefault = new DuckLakeColumnHandle(identity, INTEGER, REGULAR, Optional.of("42"));

        assertFalse(withoutDefault.equals(withDefault));
        assertFalse(withDefault.equals(withoutDefault));
    }

    @Test
    public void testSynthesizedColumnHandleJsonRoundTrip()
    {
        DuckLakeColumnIdentity identity = new DuckLakeColumnIdentity(-1, "computed", PRIMITIVE, "varchar", ImmutableList.of());
        DuckLakeColumnHandle expected = new DuckLakeColumnHandle(identity, VARCHAR, SYNTHESIZED, Optional.empty());
        assertRoundTrip(expected);
        assertEquals(expected.getColumnType(), SYNTHESIZED);
    }

    @Test
    public void testHiddenColumnHandlesJsonRoundTrip()
    {
        assertRoundTrip(DuckLakeColumnHandle.PATH_COLUMN_HANDLE);
        assertRoundTrip(DuckLakeColumnHandle.ROW_ID_COLUMN_HANDLE);
        assertRoundTrip(DuckLakeColumnHandle.ROW_POSITION_COLUMN_HANDLE);
    }

    @Test
    public void testMetadataColumnsAreMutuallyExclusive()
    {
        DuckLakeColumnHandle regular = DuckLakeColumnHandle.primitiveColumnHandle(1, "id", "int32", INTEGER);
        assertFalse(regular.isPathColumn());
        assertFalse(regular.isRowIdColumn());
        assertFalse(regular.isRowPositionColumn());

        assertTrue(DuckLakeColumnHandle.PATH_COLUMN_HANDLE.isPathColumn());
        assertFalse(DuckLakeColumnHandle.PATH_COLUMN_HANDLE.isRowIdColumn());
        assertFalse(DuckLakeColumnHandle.PATH_COLUMN_HANDLE.isRowPositionColumn());

        assertTrue(DuckLakeColumnHandle.ROW_ID_COLUMN_HANDLE.isRowIdColumn());
        assertFalse(DuckLakeColumnHandle.ROW_ID_COLUMN_HANDLE.isPathColumn());
        assertFalse(DuckLakeColumnHandle.ROW_ID_COLUMN_HANDLE.isRowPositionColumn());

        assertTrue(DuckLakeColumnHandle.ROW_POSITION_COLUMN_HANDLE.isRowPositionColumn());
        assertFalse(DuckLakeColumnHandle.ROW_POSITION_COLUMN_HANDLE.isPathColumn());
        assertFalse(DuckLakeColumnHandle.ROW_POSITION_COLUMN_HANDLE.isRowIdColumn());

        assertTrue(DuckLakeColumnHandle.isMetadataColumnId(DuckLakeColumnHandle.PATH_COLUMN_HANDLE.getId()));
        assertTrue(DuckLakeColumnHandle.isMetadataColumnId(DuckLakeColumnHandle.ROW_ID_COLUMN_HANDLE.getId()));
        assertTrue(DuckLakeColumnHandle.isMetadataColumnId(DuckLakeColumnHandle.ROW_POSITION_COLUMN_HANDLE.getId()));
        assertFalse(DuckLakeColumnHandle.isMetadataColumnId(regular.getId()));
    }

    @Test
    public void testCreateFromNestedStructIdentity()
    {
        TypeManager typeManager = createTestFunctionAndTypeManager();
        DuckLakeColumnIdentity structIdentity = structColumn();
        DuckLakeColumnHandle handle = DuckLakeColumnHandle.create(structIdentity, typeManager, Optional.empty());

        assertEquals(handle.getType(), RowType.from(ImmutableList.of(
                RowType.field("a", INTEGER),
                RowType.field("b", VARCHAR))));
    }

    @Test
    public void testTableLayoutHandleJsonRoundTrip()
    {
        DuckLakeTableHandle table = new DuckLakeTableHandle(
                "test_schema",
                DuckLakeTableName.from("orders").withSnapshotId(7, false),
                1L,
                7L,
                "/data/ducklake/orders",
                nestedSchema());

        DuckLakeColumnHandle idColumn = DuckLakeColumnHandle.primitiveColumnHandle(1, "id", "int32", INTEGER);
        DuckLakeColumnHandle nameColumn = DuckLakeColumnHandle.primitiveColumnHandle(5, "name", "varchar", VARCHAR);

        TupleDomain<ColumnHandle> domainPredicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                idColumn, Domain.singleValue(INTEGER, 5L),
                nameColumn, Domain.notNull(VARCHAR)));

        Map<String, DuckLakeColumnHandle> predicateColumns = ImmutableMap.of("id", idColumn, "name", nameColumn);
        Optional<Set<DuckLakeColumnHandle>> requestedColumns = Optional.of(ImmutableSet.of(idColumn, nameColumn));

        DuckLakeTableLayoutHandle expected = new DuckLakeTableLayoutHandle(table, domainPredicate, predicateColumns, requestedColumns);

        JsonCodec<DuckLakeTableLayoutHandle> codec = getTableLayoutHandleCodec();
        String json = codec.toJson(expected);
        DuckLakeTableLayoutHandle actual = codec.fromJson(json);

        assertEquals(actual, expected);
        assertEquals(actual.getTable(), expected.getTable());
        assertEquals(actual.getDomainPredicate(), expected.getDomainPredicate());
        assertEquals(actual.getPredicateColumns(), expected.getPredicateColumns());
        assertEquals(actual.getRequestedColumns(), expected.getRequestedColumns());
    }

    private static void assertRoundTrip(DuckLakeColumnHandle expected)
    {
        JsonCodec<DuckLakeColumnHandle> codec = getColumnHandleCodec();
        String json = codec.toJson(expected);
        DuckLakeColumnHandle actual = codec.fromJson(json);

        assertEquals(actual, expected);
        assertEquals(actual.getId(), expected.getId());
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getType(), expected.getType());
        assertEquals(actual.getColumnType(), expected.getColumnType());
        assertEquals(actual.getDefaultValue(), expected.getDefaultValue());
    }

    private static JsonCodec<DuckLakeColumnHandle> getColumnHandleCodec()
    {
        return createInjector().getInstance(new Key<JsonCodec<DuckLakeColumnHandle>>() {});
    }

    private static JsonCodec<DuckLakeTableLayoutHandle> getTableLayoutHandleCodec()
    {
        return createInjector().getInstance(new Key<JsonCodec<DuckLakeTableLayoutHandle>>() {});
    }

    /**
     * The engine-side JSON wiring a coordinator uses for connector handles: {@link
     * HandleJsonModule} for the polymorphic {@code ColumnHandle} serialization inside a {@code
     * TupleDomain} plus the {@code Type} and {@code Block} (de)serializers a column handle and a
     * domain need.
     */
    private static Injector createInjector()
    {
        Module module = binder -> {
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule());
            binder.bind(ConnectorManager.class).toProvider(Providers.of(null)).in(Scopes.SINGLETON);
            configBinder(binder).bindConfig(FeaturesConfig.class);
            FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
            binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            newSetBinder(binder, Type.class);

            binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
            newSetBinder(binder, BlockEncoding.class);
            jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
            jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
            jsonCodecBinder(binder).bindJsonCodec(DuckLakeColumnHandle.class);
            jsonCodecBinder(binder).bindJsonCodec(DuckLakeTableLayoutHandle.class);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        HandleResolver handleResolver = injector.getInstance(HandleResolver.class);
        handleResolver.addConnectorName("ducklake", new DuckLakeHandleResolver());
        return injector;
    }

    /**
     * The {@code types.nested} fixture: {@code id int32}, {@code list_col list[element int32]},
     * {@code struct_col struct[a int32, b varchar]}, {@code map_col map[key varchar, value
     * int32]}, one initial default ({@code struct_col} = {@code "42"}), and two partition fields
     * over {@code list_col} ({@code month}, {@code year}).
     */
    private static PrestoDuckLakeSchema nestedSchema()
    {
        DuckLakeColumnIdentity idColumn = primitive(1, "id", "int32");
        DuckLakeColumnIdentity listElement = primitive(6, "element", "int32");
        DuckLakeColumnIdentity listColumn = new DuckLakeColumnIdentity(2, "list_col", ARRAY, "list", ImmutableList.of(listElement));
        DuckLakeColumnIdentity structColumn = structColumn();
        DuckLakeColumnIdentity mapKey = primitive(9, "key", "varchar");
        DuckLakeColumnIdentity mapValue = primitive(10, "value", "int32");
        DuckLakeColumnIdentity mapColumn = new DuckLakeColumnIdentity(4, "map_col", MAP, "map", ImmutableList.of(mapKey, mapValue));

        List<DuckLakeColumnIdentity> columns = ImmutableList.of(idColumn, listColumn, structColumn, mapColumn);
        Map<Long, String> initialDefaults = ImmutableMap.of(3L, "42");
        List<DuckLakePartitionField> partitionFields = ImmutableList.of(
                new DuckLakePartitionField(0, 2, "month"),
                new DuckLakePartitionField(1, 2, "year"));

        return new PrestoDuckLakeSchema(columns, initialDefaults, partitionFields);
    }

    private static DuckLakeColumnIdentity structColumn()
    {
        DuckLakeColumnIdentity structA = primitive(7, "a", "int32");
        DuckLakeColumnIdentity structB = primitive(8, "b", "varchar");
        return new DuckLakeColumnIdentity(3, "struct_col", STRUCT, "struct", ImmutableList.of(structA, structB));
    }

    private static DuckLakeColumnIdentity primitive(long id, String name, String duckLakeType)
    {
        return new DuckLakeColumnIdentity(id, name, PRIMITIVE, duckLakeType, ImmutableList.of());
    }
}
