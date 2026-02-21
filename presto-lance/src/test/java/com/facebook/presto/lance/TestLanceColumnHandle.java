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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.testng.Assert.assertEquals;

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
        assertEquals(LanceColumnHandle.toPrestoType(ArrowType.Bool.INSTANCE), BOOLEAN);
        assertEquals(LanceColumnHandle.toPrestoType(new ArrowType.Int(32, true)), INTEGER);
        assertEquals(LanceColumnHandle.toPrestoType(new ArrowType.Int(64, true)), BIGINT);
        assertEquals(LanceColumnHandle.toPrestoType(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), REAL);
        assertEquals(LanceColumnHandle.toPrestoType(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), DOUBLE);
        assertEquals(LanceColumnHandle.toPrestoType(ArrowType.Utf8.INSTANCE), VARCHAR);
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

    private JsonCodec<LanceColumnHandle> getJsonCodec()
    {
        ObjectMapperProvider objectMapperProvider = new JsonObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(createTestFunctionAndTypeManager())));
        return new JsonCodecFactory(objectMapperProvider).jsonCodec(LanceColumnHandle.class);
    }
}
