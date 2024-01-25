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
package com.facebook.presto.common.block;

import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.block.BlockBuilderUtils.writePositionToBlockBuilder;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestBlockBuilderUtils
{
    private static final MapType TEST_MAP_TYPE = new MapType(
            BIGINT,
            VARCHAR,
            MethodHandleUtil.methodHandle(TestBlockBuilderUtils.class, "throwUnsupportedOperation"),
            MethodHandleUtil.methodHandle(TestBlockBuilderUtils.class, "throwUnsupportedOperation"));
    private static final Map<Long, String> TEST_MAP_VALUES = createTestMap();

    // Presto Query Engine does not support Map with Null keys.
    // Presto ORC reader and Writer are used as library in some other
    // projects, and it requires null keys to be supported in the Map.
    private static Map<Long, String> createTestMap()
    {
        Map<Long, String> testMap = new HashMap<>();
        testMap.put(1L, "ONE");
        testMap.put(2L, "TWO");
        testMap.put(null, "NULL");
        testMap.put(4L, null);
        return testMap;
    }

    @Test
    public void testArrayBlockBuilder()
    {
        long[] values = new long[]{1, 2, 3, 4, 5};

        ArrayBlockBuilder blockBuilder1 = new ArrayBlockBuilder(BIGINT, null, 1);
        BlockBuilder elementBuilder = blockBuilder1.beginBlockEntry();
        for (long value : values) {
            BIGINT.writeLong(elementBuilder, value);
        }
        Block expectedBlock = blockBuilder1.closeEntry().build();

        // write values to a new block using BlockBuilderUtil
        BlockBuilder blockBuilder2 = new ArrayBlockBuilder(BIGINT, null, 1);
        writePositionToBlockBuilder(expectedBlock, 0, blockBuilder2);
        Block newBlock = blockBuilder2.build();
        assertEquals(newBlock, expectedBlock);
    }

    @Test
    public void testMapBlockBuilder()
    {
        BlockBuilder blockBuilder1 = TEST_MAP_TYPE.createBlockBuilder(null, 1);
        BlockBuilder mapBlockBuilder = blockBuilder1.beginBlockEntry();
        writeValuesToMapBuilder(mapBlockBuilder);
        Block expectedBlock = blockBuilder1.closeEntry().build();

        // write values to a new block using BlockBuilderUtil
        BlockBuilder blockBuilder2 = TEST_MAP_TYPE.createBlockBuilder(null, 1);
        writePositionToBlockBuilder(expectedBlock, 0, blockBuilder2);
        Block newBlock = blockBuilder2.build();
        assertEquals(newBlock, expectedBlock);
    }

    @Test
    public void testRowBlockBuilder()
    {
        RowType rowType = rowType(VARCHAR, BIGINT, TEST_MAP_TYPE);
        BlockBuilder blockBuilder = rowType.createBlockBuilder(null, 1);

        BlockBuilder rowBlockBuilder = blockBuilder.beginBlockEntry();
        VARCHAR.writeString(rowBlockBuilder, "TEST_ROW");
        BIGINT.writeLong(rowBlockBuilder, 10L);
        BlockBuilder mapBlockBuilder = rowBlockBuilder.beginBlockEntry();
        writeValuesToMapBuilder(mapBlockBuilder);
        rowBlockBuilder.closeEntry();
        Block expectedBlock = blockBuilder.closeEntry().build();

        // write values to a new block using BlockBuilderUtil
        BlockBuilder blockBuilder2 = rowType.createBlockBuilder(null, 1);
        writePositionToBlockBuilder(expectedBlock, 0, blockBuilder2);
        Block newBlock = blockBuilder2.build();
        assertEquals(newBlock, expectedBlock);
    }

    private static void writeValuesToMapBuilder(BlockBuilder mapBlockBuilder)
    {
        for (Map.Entry<Long, String> entry : TEST_MAP_VALUES.entrySet()) {
            Long key = entry.getKey();
            if (key == null) {
                mapBlockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(mapBlockBuilder, entry.getKey());
            }

            String value = entry.getValue();
            if (value == null) {
                mapBlockBuilder.appendNull();
            }
            else {
                VARCHAR.writeString(mapBlockBuilder, entry.getValue());
            }
        }
    }

    public static RowType rowType(Type... fieldTypes)
    {
        List<RowType.Field> fields = new ArrayList<>();
        for (int i = 0; i < fieldTypes.length; i++) {
            Type fieldType = fieldTypes[i];
            String fieldName = "field_" + i;
            fields.add(new RowType.Field(Optional.of(fieldName), fieldType));
        }
        return RowType.from(fields);
    }

    // Used via reflection for creating mapType.
    public static void throwUnsupportedOperation()
    {
        throw new UnsupportedOperationException();
    }
}
