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
package com.facebook.presto.orc;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.metadata.CompressionKind;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.createCustomOrcRecordReader;
import static com.facebook.presto.orc.OrcTester.createCustomOrcSelectiveRecordReader;
import static com.facebook.presto.orc.OrcTester.createOrcRecordWriter;
import static com.facebook.presto.orc.OrcTester.createSettableStructObjectInspector;
import static com.facebook.presto.testing.TestingEnvironment.getOperatorMethodHandle;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

public class TestOrcMapNullKey
{
    @DataProvider(name = "mapNullKeysEnabled")
    public static Object[][] mapNullKeysEnabled()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "mapNullKeysEnabled")
    public void testMapTypeWithNullsWithBatchReader(boolean mapNullKeysEnabled)
            throws Exception
    {
        MapType mapType = createMapType(BIGINT, BIGINT);

        Map<Long, Long> map = generateMap();

        Map<Long, Long> expectedToRead = new HashMap<>(map);
        if (!mapNullKeysEnabled) {
            expectedToRead.remove(null);
        }

        try (TempFile tempFile = createSingleColumnMapFileWithNullValues(mapType, map)) {
            try (OrcBatchRecordReader reader = createCustomOrcRecordReader(tempFile, ORC, OrcPredicate.TRUE, mapType, INITIAL_BATCH_SIZE, false, mapNullKeysEnabled)) {
                int batchSize = reader.nextBatch();
                assertEquals(batchSize, 1);

                assertEquals(readMap(reader.readBlock(0), 0), expectedToRead);

                assertEquals(reader.nextBatch(), -1);
            }
        }
    }

    @Test(dataProvider = "mapNullKeysEnabled")
    public void testMapTypeWithNullsWithSelectiveReader(boolean mapNullKeysEnabled)
            throws Exception
    {
        MapType mapType = createMapType(BIGINT, BIGINT);

        Map<Long, Long> map = generateMap();

        Map<Long, Long> expectedToRead = new HashMap<>(map);
        if (!mapNullKeysEnabled) {
            expectedToRead.remove(null);
        }

        try (TempFile tempFile = createSingleColumnMapFileWithNullValues(mapType, map)) {
            try (OrcSelectiveRecordReader reader = createCustomOrcSelectiveRecordReader(
                    tempFile,
                    ORC,
                    OrcPredicate.TRUE,
                    mapType,
                    INITIAL_BATCH_SIZE,
                    mapNullKeysEnabled)) {
                assertEquals(readMap(reader.getNextPage().getBlock(0).getLoadedBlock(), 0), expectedToRead);

                assertNull(reader.getNextPage());
            }
        }
    }

    private static Map<Long, Long> generateMap()
    {
        Map<Long, Long> map = new HashMap<>();

        for (long i = 0; i < 10; i++) {
            map.put(i, i + 1);
            map.put(-i, null);
        }
        map.put(null, 0L);
        return map;
    }

    private static Map<Long, Long> readMap(Block block, int rowId)
    {
        ColumnarMap columnarMap = ColumnarMap.toColumnarMap(block);
        assertFalse(columnarMap.isNull(rowId));

        Block keysBlock = columnarMap.getKeysBlock();
        Block valuesBlock = columnarMap.getValuesBlock();

        Map<Long, Long> actual = new HashMap<>();
        for (int i = 0; i < columnarMap.getEntryCount(rowId); i++) {
            int position = columnarMap.getOffset(rowId) + i;
            Long key = keysBlock.isNull(position) ? null : BIGINT.getLong(keysBlock, position);
            Long value = valuesBlock.isNull(position) ? null : BIGINT.getLong(valuesBlock, position);
            actual.put(key, value);
        }
        return actual;
    }

    public static MapType createMapType(Type keyType, Type valueType)
    {
        MethodHandle keyNativeEquals = getOperatorMethodHandle(OperatorType.EQUAL, keyType, keyType);
        MethodHandle keyBlockEquals = compose(keyNativeEquals, nativeValueGetter(keyType), nativeValueGetter(keyType));
        MethodHandle keyNativeHashCode = getOperatorMethodHandle(OperatorType.HASH_CODE, keyType);
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));

        return new MapType(
                keyType,
                valueType,
                keyBlockEquals,
                keyBlockHashCode);
    }

    private static TempFile createSingleColumnMapFileWithNullValues(Type mapType, Map<Long, Long> map)
            throws IOException
    {
        OrcSerde serde = new OrcSerde();
        TempFile tempFile = new TempFile();
        FileSinkOperator.RecordWriter writer = createOrcRecordWriter(tempFile.getFile(), ORC_12, CompressionKind.NONE, mapType);
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", mapType);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        objectInspector.setStructFieldData(row, field, map);
        Writable record = serde.serialize(row, objectInspector);
        writer.write(record);
        writer.close(false);
        return tempFile;
    }
}
