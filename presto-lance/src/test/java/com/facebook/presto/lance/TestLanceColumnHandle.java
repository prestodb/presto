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
package com.facebook.presto.lance;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.json.ObjectMapperProvider;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLanceColumnHandle
{
    @Test
    public void testJsonRoundTrip()
    {
        LanceColumnHandle handle = new LanceColumnHandle("col1", BIGINT, true);
        JsonCodec<LanceColumnHandle> codec = getJsonCodec();
        String json = codec.toJson(handle);
        LanceColumnHandle copy = codec.fromJson(json);
        assertEquals(copy, handle);
        assertEquals(copy.getColumnName(), "col1");
        assertEquals(copy.getColumnType(), BIGINT);
        assertEquals(copy.isNullable(), true);
    }

    @Test
    public void testArrowToPrestoType()
    {
        assertEquals(LanceColumnHandle.toPrestoType(field("a", ArrowType.Bool.INSTANCE)), BOOLEAN);
        assertEquals(LanceColumnHandle.toPrestoType(field("b", new ArrowType.Int(32, true))), INTEGER);
        assertEquals(LanceColumnHandle.toPrestoType(field("c", new ArrowType.Int(64, true))), BIGINT);
        assertEquals(LanceColumnHandle.toPrestoType(field("d", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))), REAL);
        assertEquals(LanceColumnHandle.toPrestoType(field("d2", new ArrowType.FloatingPoint(FloatingPointPrecision.HALF))), REAL);
        assertEquals(LanceColumnHandle.toPrestoType(field("e", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))), DOUBLE);
        assertEquals(LanceColumnHandle.toPrestoType(field("f", ArrowType.Utf8.INSTANCE)), VARCHAR);
    }

    @Test
    public void testPrestoToArrowType()
    {
        assertEquals(LanceColumnHandle.toArrowType(BOOLEAN), ArrowType.Bool.INSTANCE);
        assertEquals(LanceColumnHandle.toArrowType(INTEGER), new ArrowType.Int(32, true));
        assertEquals(LanceColumnHandle.toArrowType(BIGINT), new ArrowType.Int(64, true));
        assertEquals(LanceColumnHandle.toArrowType(REAL), new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        assertEquals(LanceColumnHandle.toArrowType(DOUBLE), new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        assertEquals(LanceColumnHandle.toArrowType(VARCHAR), ArrowType.Utf8.INSTANCE);
    }

    @Test
    public void testRowAddressColumn()
    {
        LanceColumnHandle handle = LanceColumnHandle.rowAddressColumn();
        assertEquals(handle.getColumnName(), "_rowaddr");
        assertEquals(handle.getColumnType(), BIGINT);
        assertEquals(handle.getColumnHandleType(), LanceColumnHandle.ColumnHandleType.ROW_ADDRESS);
        assertTrue(handle.isRowAddress());
        assertFalse(handle.isUpdateRowId());
    }

    @Test
    public void testUpdateRowIdColumnStructShape()
    {
        LanceColumnHandle id = new LanceColumnHandle("id", BIGINT);
        LanceColumnHandle name = new LanceColumnHandle("name", VARCHAR);
        LanceColumnHandle rowId = LanceColumnHandle.updateRowIdColumn(ImmutableList.of(id, name));

        assertEquals(rowId.getColumnName(), "$row_id");
        assertEquals(rowId.getColumnHandleType(), LanceColumnHandle.ColumnHandleType.UPDATE_ROW_ID);
        assertTrue(rowId.isUpdateRowId());

        RowType rowType = (RowType) rowId.getColumnType();
        List<RowType.Field> fields = rowType.getFields();
        // First field is always the row address, followed by the unmodified columns in order.
        assertEquals(fields.size(), 3);
        assertEquals(fields.get(0).getName().get(), "_rowaddr");
        assertEquals(fields.get(0).getType(), BIGINT);
        assertEquals(fields.get(1).getName().get(), "id");
        assertEquals(fields.get(2).getName().get(), "name");
    }

    private static Field field(String name, ArrowType type)
    {
        return new Field(name, new FieldType(true, type, null), Collections.emptyList());
    }

    private JsonCodec<LanceColumnHandle> getJsonCodec()
    {
        ObjectMapperProvider objectMapperProvider = new JsonObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(createTestFunctionAndTypeManager())));
        return new JsonCodecFactory(objectMapperProvider).jsonCodec(LanceColumnHandle.class);
    }
}
