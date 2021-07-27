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
package com.facebook.presto.maxcompute.util;

import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.aliyun.odps.type.TypeInfoFactory.BOOLEAN;
import static com.aliyun.odps.type.TypeInfoFactory.FLOAT;
import static com.aliyun.odps.type.TypeInfoFactory.TIMESTAMP;
import static com.aliyun.odps.type.TypeInfoFactory.TINYINT;
import static com.aliyun.odps.type.TypeInfoFactory.getArrayTypeInfo;
import static com.aliyun.odps.type.TypeInfoFactory.getCharTypeInfo;
import static com.aliyun.odps.type.TypeInfoFactory.getMapTypeInfo;
import static com.aliyun.odps.type.TypeInfoFactory.getStructTypeInfo;
import static com.aliyun.odps.type.TypeInfoFactory.getVarcharTypeInfo;
import static com.facebook.presto.maxcompute.util.MaxComputeReadUtils.serializeObject;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestMaxComputeReadUtils
{
    private static final TypeManager TYPE_MANAGER = createTestFunctionAndTypeManager();

    @Test
    public void testPrimitive()
    {
        IntegerType type = IntegerType.INTEGER;
        BlockBuilder builder = type.createBlockBuilder(null, 1);
        serializeObject(type, builder, 13);

        Block block = builder.build();
        assertEquals(13, type.getLong(block, 0));
    }

    @Test
    public void testArray()
    {
        Type type = new ArrayType(TinyintType.TINYINT);
        List<Byte> list = Lists.newArrayList((byte) 10, (byte) 3, (byte) 4, (byte) 6);
        Block block = serializeObject(type, null, list);

        Object o = MaxComputeWriteUtils.deSerializeObject(type, getArrayTypeInfo(TINYINT), block);
        assertEquals(o, list);
    }

    @Test
    public void testMap()
    {
        Type type = DataTypes.convertToPrestoType("MAP<VARCHAR(20), FLOAT>", TYPE_MANAGER);
        Map<String, Float> map = new HashMap<>();
        map.put("AAAA", 3.88f);
        map.put("BBBB", 36.79786543f);
        map.put("Cccc", 23.4356732f);

        Block block = serializeObject(type, null, map);

        VarcharTypeInfo keyTypeInfo = getVarcharTypeInfo(20);
        Object o = MaxComputeWriteUtils.deSerializeObject(type, getMapTypeInfo(keyTypeInfo, FLOAT), block);
        assertTrue(o instanceof Map<?, ?>);
        assertEquals(o.toString(), map.toString());
    }

    @Test
    public void testStruct()
    {
        Type type = DataTypes.convertToPrestoType("struct<aa:double, bb:timestamp, cc:char(10)>", TYPE_MANAGER);
        Timestamp timestamp = Timestamp.valueOf("2020-06-01 12:23:45");
        Char char10 = new Char("minghui", 10);
        List<Object> row = Lists.newArrayList(6.888, timestamp, char10);

        ArrayList<String> names = Lists.newArrayList("aa", "bb", "cc");
        StructTypeInfo structTypeInfo = getStructTypeInfo(names, Lists.newArrayList(BOOLEAN, TIMESTAMP, getCharTypeInfo(10)));
        SimpleStruct simpleStruct = new SimpleStruct(structTypeInfo, row);

        Block block = serializeObject(type, null, simpleStruct);

        Object o = MaxComputeWriteUtils.deSerializeObject(type, structTypeInfo, block);

        assertTrue(o instanceof SimpleStruct);
        SimpleStruct result = (SimpleStruct) o;
        assertEquals(simpleStruct.getFieldCount(), result.getFieldCount());

        Object value1 = result.getFieldValue("aa");
        assertTrue(value1 instanceof Double);
        assertEquals((double) value1, 6.888, 0.00001);

        Object value2 = result.getFieldValue("bb");
        assertTrue(value2 instanceof Timestamp);
        assertEquals(value2, timestamp);

        Object value3 = result.getFieldValue("cc");
        assertTrue(value3 instanceof Char);
        assertEquals(value3.toString(), Strings.padEnd(char10.toString(), 10, ' '));
    }
}
