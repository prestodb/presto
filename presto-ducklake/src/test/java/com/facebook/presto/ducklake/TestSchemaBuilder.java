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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.ducklake.DuckLakeColumnIdentity.TypeCategory;
import com.facebook.presto.ducklake.catalog.DuckLakeColumnRow;
import com.facebook.presto.ducklake.catalog.DuckLakePartitionField;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class TestSchemaBuilder
{
    @Test
    public void testNestedTree()
    {
        List<DuckLakeColumnIdentity> actual = SchemaBuilder.buildColumns(nestedRows());
        assertEquals(actual, expectedNestedColumns());
    }

    @Test
    public void testRowsOutOfColumnOrderStillBuildCorrectOrder()
    {
        List<DuckLakeColumnRow> shuffled = Lists.reverse(nestedRows());
        List<DuckLakeColumnIdentity> actual = SchemaBuilder.buildColumns(shuffled);
        assertEquals(actual, expectedNestedColumns());
    }

    @Test
    public void testEvoTableLatestSnapshotKeepsColumnIdAcrossRename()
    {
        List<DuckLakeColumnRow> rows = ImmutableList.of(
                row(1, 1, "id", "int32", Optional.empty(), OptionalLong.empty()),
                row(2, 2, "full_name", "varchar", Optional.empty(), OptionalLong.empty()),
                row(3, 3, "score", "int32", Optional.of("42"), OptionalLong.empty()));

        PrestoDuckLakeSchema schema = SchemaBuilder.buildSchema(rows, ImmutableList.of());

        assertEquals(schema.getColumns().size(), 3);
        assertEquals(schema.getColumns().get(1).getId(), 2L);
        assertEquals(schema.getColumns().get(1).getName(), "full_name");
        assertEquals(schema.getInitialDefaults(), ImmutableMap.of(3L, "42"));
    }

    @Test
    public void testEvoTableCreationSnapshotColumnIdMatchesLatest()
    {
        List<DuckLakeColumnRow> rows = ImmutableList.of(
                row(1, 1, "id", "int32", Optional.empty(), OptionalLong.empty()),
                row(2, 2, "name", "varchar", Optional.empty(), OptionalLong.empty()));

        PrestoDuckLakeSchema schema = SchemaBuilder.buildSchema(rows, ImmutableList.of());

        assertEquals(schema.getColumns().size(), 2);
        assertEquals(schema.getColumns().get(1).getId(), 2L);
        assertEquals(schema.getColumns().get(1).getName(), "name");
        assertEquals(schema.getInitialDefaults(), ImmutableMap.of());
    }

    @Test
    public void testMissingParentThrows()
    {
        List<DuckLakeColumnRow> rows = ImmutableList.of(
                row(1, 1, "id", "int32", Optional.empty(), OptionalLong.empty()),
                row(2, 2, "orphan", "int32", Optional.empty(), OptionalLong.of(999)));

        PrestoException exception = expectThrows(PrestoException.class, () -> SchemaBuilder.buildColumns(rows));
        assertEquals(exception.getErrorCode(), DUCKLAKE_INVALID_METADATA.toErrorCode());
    }

    @Test
    public void testCycleThrows()
    {
        List<DuckLakeColumnRow> rows = ImmutableList.of(
                row(1, 1, "a", "struct", Optional.empty(), OptionalLong.of(2)),
                row(2, 2, "b", "struct", Optional.empty(), OptionalLong.of(1)));

        PrestoException exception = expectThrows(PrestoException.class, () -> SchemaBuilder.buildColumns(rows));
        assertEquals(exception.getErrorCode(), DUCKLAKE_INVALID_METADATA.toErrorCode());
    }

    @Test
    public void testPrestoDuckLakeSchemaJsonRoundTrip()
    {
        List<DuckLakePartitionField> partitionFields = ImmutableList.of(
                new DuckLakePartitionField(0, 1, "identity"),
                new DuckLakePartitionField(1, 2, "bucket(16)"));

        PrestoDuckLakeSchema expected = SchemaBuilder.buildSchema(evoLatestRows(), partitionFields);

        JsonCodec<PrestoDuckLakeSchema> codec = jsonCodec(PrestoDuckLakeSchema.class);
        PrestoDuckLakeSchema actual = codec.fromJson(codec.toJson(expected));

        assertEquals(actual, expected);
    }

    private static List<DuckLakeColumnRow> evoLatestRows()
    {
        return ImmutableList.of(
                row(1, 1, "id", "int32", Optional.empty(), OptionalLong.empty()),
                row(2, 2, "full_name", "varchar", Optional.empty(), OptionalLong.empty()),
                row(3, 3, "score", "int32", Optional.of("42"), OptionalLong.empty()));
    }

    private static List<DuckLakeColumnRow> nestedRows()
    {
        return ImmutableList.of(
                row(1, 1, "id", "int32", Optional.empty(), OptionalLong.empty()),
                row(2, 2, "list_col", "list", Optional.empty(), OptionalLong.empty()),
                row(3, 3, "element", "int32", Optional.empty(), OptionalLong.of(2)),
                row(4, 4, "struct_col", "struct", Optional.empty(), OptionalLong.empty()),
                row(5, 5, "a", "int32", Optional.empty(), OptionalLong.of(4)),
                row(6, 6, "b", "varchar", Optional.empty(), OptionalLong.of(4)),
                row(7, 7, "map_col", "map", Optional.empty(), OptionalLong.empty()),
                row(8, 8, "key", "varchar", Optional.empty(), OptionalLong.of(7)),
                row(9, 9, "value", "int32", Optional.empty(), OptionalLong.of(7)),
                row(10, 10, "struct_with_list", "struct", Optional.empty(), OptionalLong.empty()),
                row(11, 11, "name", "varchar", Optional.empty(), OptionalLong.of(10)),
                row(12, 12, "tags", "list", Optional.empty(), OptionalLong.of(10)),
                row(13, 13, "element", "varchar", Optional.empty(), OptionalLong.of(12)));
    }

    private static List<DuckLakeColumnIdentity> expectedNestedColumns()
    {
        DuckLakeColumnIdentity id = primitive(1, "id", "int32");
        DuckLakeColumnIdentity listCol = new DuckLakeColumnIdentity(2, "list_col", TypeCategory.ARRAY, "list",
                ImmutableList.of(primitive(3, "element", "int32")));
        DuckLakeColumnIdentity structCol = new DuckLakeColumnIdentity(4, "struct_col", TypeCategory.STRUCT, "struct",
                ImmutableList.of(primitive(5, "a", "int32"), primitive(6, "b", "varchar")));
        DuckLakeColumnIdentity mapCol = new DuckLakeColumnIdentity(7, "map_col", TypeCategory.MAP, "map",
                ImmutableList.of(primitive(8, "key", "varchar"), primitive(9, "value", "int32")));
        DuckLakeColumnIdentity structWithList = new DuckLakeColumnIdentity(10, "struct_with_list", TypeCategory.STRUCT, "struct",
                ImmutableList.of(
                        primitive(11, "name", "varchar"),
                        new DuckLakeColumnIdentity(12, "tags", TypeCategory.ARRAY, "list",
                                ImmutableList.of(primitive(13, "element", "varchar")))));
        return ImmutableList.of(id, listCol, structCol, mapCol, structWithList);
    }

    private static DuckLakeColumnIdentity primitive(long id, String name, String duckLakeType)
    {
        return new DuckLakeColumnIdentity(id, name, TypeCategory.PRIMITIVE, duckLakeType, ImmutableList.of());
    }

    private static DuckLakeColumnRow row(
            long columnId,
            long columnOrder,
            String columnName,
            String columnType,
            Optional<String> initialDefault,
            OptionalLong parentColumn)
    {
        return new DuckLakeColumnRow(columnId, columnOrder, columnName, columnType, initialDefault, true, parentColumn);
    }
}
