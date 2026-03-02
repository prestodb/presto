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
package com.facebook.presto.iceberg;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.json.ObjectMapperProvider;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.ARRAY;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.STRUCT;
import static com.facebook.presto.iceberg.IcebergColumnHandle.LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_HANDLE;
import static com.facebook.presto.iceberg.IcebergColumnHandle.ROW_ID_COLUMN_HANDLE;
import static com.facebook.presto.iceberg.IcebergColumnHandle.primitiveIcebergColumnHandle;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.apache.iceberg.MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER;
import static org.apache.iceberg.MetadataColumns.ROW_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestIcebergColumnHandle
{
    @Test
    public void testRoundTrip()
    {
        testRoundTrip(primitiveIcebergColumnHandle(12, "blah", BIGINT, Optional.of("this is a comment")));

        // Nested column
        ColumnIdentity foo1 = new ColumnIdentity(1, "foo1", PRIMITIVE, ImmutableList.of());
        ColumnIdentity foo2 = new ColumnIdentity(2, "foo2", PRIMITIVE, ImmutableList.of());
        ColumnIdentity foo3 = new ColumnIdentity(3, "foo3", ARRAY, ImmutableList.of(foo1));
        IcebergColumnHandle nestedColumn = new IcebergColumnHandle(
                new ColumnIdentity(
                        5,
                        "foo5",
                        STRUCT,
                        ImmutableList.of(foo2, foo3)),
                RowType.from(ImmutableList.of(
                        RowType.field("foo2", BIGINT),
                        RowType.field("foo3", new ArrayType(BIGINT)))),
                Optional.empty(),
                REGULAR);
        testRoundTrip(nestedColumn);
    }

    @Test
    public void testRowLineageColumnHandles()
    {
        // Verify ROW_ID column handle
        assertEquals(ROW_ID_COLUMN_HANDLE.getName(), ROW_ID.name());
        assertEquals(ROW_ID_COLUMN_HANDLE.getId(), ROW_ID.fieldId());
        assertEquals(ROW_ID_COLUMN_HANDLE.getType(), BIGINT);
        assertTrue(ROW_ID_COLUMN_HANDLE.isRowIdColumn());
        assertFalse(ROW_ID_COLUMN_HANDLE.isLastUpdatedSequenceNumberColumn());

        // Verify LAST_UPDATED_SEQUENCE_NUMBER column handle
        assertEquals(LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_HANDLE.getName(), LAST_UPDATED_SEQUENCE_NUMBER.name());
        assertEquals(LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_HANDLE.getId(), LAST_UPDATED_SEQUENCE_NUMBER.fieldId());
        assertEquals(LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_HANDLE.getType(), BIGINT);
        assertTrue(LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_HANDLE.isLastUpdatedSequenceNumberColumn());
        assertFalse(LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_HANDLE.isRowIdColumn());

        // Verify that _row_id IS treated as a metadata column ID so it is excluded from
        // predicate pushdown to the Iceberg scan (the library can't handle it as a partition column)
        assertTrue(IcebergMetadataColumn.isMetadataColumnId(ROW_ID_COLUMN_HANDLE.getId()));
        // Verify that _last_updated_sequence_number IS treated as a metadata column ID so that
        // IcebergPartitionInsertingPageSource can inject it as a per-file constant
        assertTrue(IcebergMetadataColumn.isMetadataColumnId(LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_HANDLE.getId()));
    }

    private void testRoundTrip(IcebergColumnHandle expected)
    {
        ObjectMapperProvider objectMapperProvider = new JsonObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(createTestFunctionAndTypeManager())));
        JsonCodec<IcebergColumnHandle> codec = new JsonCodecFactory(objectMapperProvider).jsonCodec(IcebergColumnHandle.class);
        String json = codec.toJson(expected);
        IcebergColumnHandle actual = codec.fromJson(json);

        assertEquals(actual, expected);
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getColumnIdentity(), expected.getColumnIdentity());
        assertEquals(actual.getId(), actual.getId());
        assertEquals(actual.getType(), expected.getType());
        assertEquals(actual.getComment(), expected.getComment());
    }
}
