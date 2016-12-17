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
package com.facebook.presto.hive;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.DefaultHivePartitioner;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFHash;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.collect.Maps.immutableEntry;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.toIntExact;
import static java.util.Arrays.asList;
import static java.util.Map.Entry;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveBucketing
{
    private static final TypeRegistry typeRegistry = new TypeRegistry();

    @Test
    public void testHashingBooleanLong()
            throws Exception
    {
        List<Entry<ObjectInspector, Object>> bindings = ImmutableList.<Entry<ObjectInspector, Object>>builder()
                .add(entry(javaBooleanObjectInspector, true))
                .add(entry(javaLongObjectInspector, 123L))
                .build();

        Optional<HiveBucket> bucket = HiveBucketing.getHiveBucket(bindings, 32);
        assertTrue(bucket.isPresent());
        assertEquals(bucket.get().getBucketCount(), 32);
        assertEquals(bucket.get().getBucketNumber(), 26);
    }

    @Test
    public void testHashingString()
            throws Exception
    {
        List<Entry<ObjectInspector, Object>> bindings = ImmutableList.<Entry<ObjectInspector, Object>>builder()
                .add(entry(javaStringObjectInspector, utf8Slice("sequencefile test")))
                .build();

        Optional<HiveBucket> bucket = HiveBucketing.getHiveBucket(bindings, 32);
        assertTrue(bucket.isPresent());
        assertEquals(bucket.get().getBucketCount(), 32);
        assertEquals(bucket.get().getBucketNumber(), 21);
    }

    private static Entry<ObjectInspector, Object> entry(ObjectInspector inspector, Object value)
    {
        return immutableEntry(inspector, value);
    }

    @Test
    public void testHashingCompare()
            throws Exception
    {
        assertBucketEquals("boolean", null);
        assertBucketEquals("boolean", true);
        assertBucketEquals("boolean", false);
        assertBucketEquals("tinyint", null);
        assertBucketEquals("tinyint", (byte) 5);
        assertBucketEquals("tinyint", Byte.MIN_VALUE);
        assertBucketEquals("tinyint", Byte.MAX_VALUE);
        assertBucketEquals("smallint", null);
        assertBucketEquals("smallint", (short) 300);
        assertBucketEquals("smallint", Short.MIN_VALUE);
        assertBucketEquals("smallint", Short.MAX_VALUE);
        assertBucketEquals("int", null);
        assertBucketEquals("int", 300_000);
        assertBucketEquals("int", Integer.MIN_VALUE);
        assertBucketEquals("int", Integer.MAX_VALUE);
        assertBucketEquals("bigint", null);
        assertBucketEquals("bigint", 300_000_000_000L);
        assertBucketEquals("bigint", Long.MIN_VALUE);
        assertBucketEquals("bigint", Long.MAX_VALUE);
        assertBucketEquals("float", null);
        assertBucketEquals("float", 12.34F);
        assertBucketEquals("float", -Float.MAX_VALUE);
        assertBucketEquals("float", Float.MIN_VALUE);
        assertBucketEquals("float", Float.POSITIVE_INFINITY);
        assertBucketEquals("float", Float.NEGATIVE_INFINITY);
        assertBucketEquals("double", null);
        assertBucketEquals("double", 12.34);
        assertBucketEquals("double", -Double.MAX_VALUE);
        assertBucketEquals("double", Double.MIN_VALUE);
        assertBucketEquals("double", Double.POSITIVE_INFINITY);
        assertBucketEquals("double", Double.NEGATIVE_INFINITY);
        assertBucketEquals("varchar(15)", null);
        assertBucketEquals("varchar(15)", "");
        assertBucketEquals("varchar(15)", "test string");
        assertBucketEquals("varchar(15)", "\u5f3a\u5927\u7684Presto\u5f15\u64ce"); // 3-byte UTF-8 sequences (in Basic Plane, i.e. Plane 0)
        assertBucketEquals("varchar(15)", "\uD843\uDFFC\uD843\uDFFD\uD843\uDFFE\uD843\uDFFF"); // 4 code points: 20FFC - 20FFF. 4-byte UTF-8 sequences in Supplementary Plane 2
        assertBucketEquals("string", null);
        assertBucketEquals("string", "");
        assertBucketEquals("string", "test string");
        assertBucketEquals("string", "\u5f3a\u5927\u7684Presto\u5f15\u64ce"); // 3-byte UTF-8 sequences (in Basic Plane, i.e. Plane 0)
        assertBucketEquals("string", "\uD843\uDFFC\uD843\uDFFD\uD843\uDFFE\uD843\uDFFF"); // 4 code points: 20FFC - 20FFF. 4-byte UTF-8 sequences in Supplementary Plane 2
        assertBucketEquals("date", null);
        assertBucketEquals("date", new DateWritable(toIntExact(LocalDate.of(1970, 1, 1).toEpochDay())).get());
        assertBucketEquals("date", new DateWritable(toIntExact(LocalDate.of(2015, 11, 19).toEpochDay())).get());
        assertBucketEquals("date", new DateWritable(toIntExact(LocalDate.of(1950, 11, 19).toEpochDay())).get());
        assertBucketEquals("timestamp", null);
        assertBucketEquals("timestamp", new Timestamp(1000 * LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0).toEpochSecond(ZoneOffset.UTC)));
        assertBucketEquals("timestamp", new Timestamp(1000 * LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_000_000).toEpochSecond(ZoneOffset.UTC)));
        assertBucketEquals("timestamp", new Timestamp(1000 * LocalDateTime.of(1950, 11, 19, 12, 34, 56, 789_000_000).toEpochSecond(ZoneOffset.UTC)));
        assertBucketEquals("timestamp", new Timestamp(1000 * LocalDateTime.of(2015, 11, 19, 7, 6, 5, 432_000_000).toEpochSecond(ZoneOffset.UTC)));
        assertBucketEquals("array<double>", null);
        assertBucketEquals("array<boolean>", ImmutableList.of());
        assertBucketEquals("array<smallint>", ImmutableList.of((short) 5, (short) 8, (short) 13));
        assertBucketEquals("array<string>", ImmutableList.of("test1", "test2", "test3", "test4"));
        assertBucketEquals("map<float,date>", null);
        assertBucketEquals("map<double,timestamp>", ImmutableMap.of());
        assertBucketEquals("map<string,bigint>", ImmutableMap.of("key", 123L, "key2", 123456789L, "key3", -123456L));
        assertBucketEquals("array<array<bigint>>", ImmutableList.of(ImmutableList.of(10L, 20L), ImmutableList.of(-10L, -20L), asList((Object) null)));
        assertBucketEquals("map<array<double>,map<int,timestamp>>", ImmutableMap.of(ImmutableList.of(12.3, 45.7), ImmutableMap.of(123, new Timestamp(1_234_567_890_000L))));

        // multiple bucketing columns
        assertBucketEquals(
                ImmutableList.of("float", "array<smallint>", "map<string,bigint>"),
                ImmutableList.of(12.34F, ImmutableList.of((short) 5, (short) 8, (short) 13), ImmutableMap.of("key", 123L))
        );
        assertBucketEquals(
                ImmutableList.of("double", "array<smallint>", "boolean", "map<string,bigint>", "tinyint"),
                asList(null, ImmutableList.of((short) 5, (short) 8, (short) 13), null, ImmutableMap.of("key", 123L), null)
        );
    }

    private static void assertBucketEquals(String hiveTypeStrings, Object javaValues)
            throws HiveException
    {
        // Use asList to allow nulls
        assertBucketEquals(ImmutableList.of(hiveTypeStrings), asList(javaValues));
    }

    private static void assertBucketEquals(List<String> hiveTypeStrings, List<Object> javaValues)
            throws HiveException
    {
        List<HiveType> hiveTypes = hiveTypeStrings.stream()
                .map(HiveType::valueOf)
                .collect(toImmutableList());
        List<TypeInfo> hiveTypeInfos = hiveTypes.stream()
                .map(HiveType::getTypeInfo)
                .collect(toImmutableList());

        for (int bucketCount : new int[] {1, 2, 500, 997}) {
            int actual = computeActual(hiveTypeStrings, javaValues, bucketCount, hiveTypes, hiveTypeInfos);
            int expected = computeExpected(hiveTypeStrings, javaValues, bucketCount, hiveTypeInfos);
            assertEquals(actual, expected);
        }
    }

    private static int computeExpected(List<String> hiveTypeStrings, List<Object> javaValues, int bucketCount, List<TypeInfo> hiveTypeInfos)
            throws HiveException
    {
        ImmutableList.Builder<Entry<ObjectInspector, Object>> columnBindingsBuilder = ImmutableList.builder();
        for (int i = 0; i < hiveTypeStrings.size(); i++) {
            Object javaValue = javaValues.get(i);

            columnBindingsBuilder.add(Maps.immutableEntry(
                    TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(hiveTypeInfos.get(i)),
                    javaValue));
        }
        return getHiveBucket(columnBindingsBuilder.build(), bucketCount);
    }

    private static int computeActual(List<String> hiveTypeStrings, List<Object> javaValues, int bucketCount, List<HiveType> hiveTypes, List<TypeInfo> hiveTypeInfos)
    {
        ImmutableList.Builder<Block> blockListBuilder = ImmutableList.builder();
        for (int i = 0; i < hiveTypeStrings.size(); i++) {
            Object javaValue = javaValues.get(i);
            Type type = hiveTypes.get(i).getType(typeRegistry);

            BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 3);
            // prepend 2 nulls to make sure position is respected when HiveBucketing function
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            appendToBlockBuilder(type, javaValue, blockBuilder);
            Block block = blockBuilder.build();
            blockListBuilder.add(block);
        }
        ImmutableList<Block> blockList = blockListBuilder.build();
        return HiveBucketing.getHiveBucket(hiveTypeInfos, new Page(blockList.toArray(new Block[blockList.size()])), 2, bucketCount);
    }

    public static int getHiveBucket(List<Entry<ObjectInspector, Object>> columnBindings, int bucketCount)
            throws HiveException
    {
        GenericUDFHash udf = new GenericUDFHash();
        ObjectInspector[] objectInspectors = new ObjectInspector[columnBindings.size()];
        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[columnBindings.size()];

        int i = 0;
        for (Entry<ObjectInspector, Object> entry : columnBindings) {
            objectInspectors[i] = entry.getKey();
            if (entry.getValue() != null && entry.getKey() instanceof JavaHiveVarcharObjectInspector) {
                JavaHiveVarcharObjectInspector varcharObjectInspector = (JavaHiveVarcharObjectInspector) entry.getKey();
                deferredObjects[i] = new GenericUDF.DeferredJavaObject(new HiveVarchar(((String) entry.getValue()), varcharObjectInspector.getMaxLength()));
            }
            else {
                deferredObjects[i] = new GenericUDF.DeferredJavaObject(entry.getValue());
            }
            i++;
        }

        ObjectInspector udfInspector = udf.initialize(objectInspectors);
        IntObjectInspector inspector = checkType(udfInspector, IntObjectInspector.class, "udfInspector");

        Object result = udf.evaluate(deferredObjects);
        HiveKey hiveKey = new HiveKey();
        hiveKey.setHashCode(inspector.get(result));

        return new DefaultHivePartitioner<>().getBucket(hiveKey, null, bucketCount);
    }

    /**
     * @param element java representation of the element (e.g. int, double, List, Date, String) to be added
     */
    public static void appendToBlockBuilder(Type type, Object element, BlockBuilder blockBuilder)
    {
        String typeBase = type.getTypeSignature().getBase();
        if (element == null) {
            blockBuilder.appendNull();
            return;
        }
        switch (typeBase) {
            case StandardTypes.ARRAY: {
                BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
                for (Object subElement : (Iterable<?>) element) {
                    appendToBlockBuilder(type.getTypeParameters().get(0), subElement, subBlockBuilder);
                }
                blockBuilder.closeEntry();
                break;
            }
            case StandardTypes.ROW: {
                BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
                int field = 0;
                for (Object subElement : (Iterable<?>) element) {
                    appendToBlockBuilder(type.getTypeParameters().get(field), subElement, subBlockBuilder);
                    field++;
                }
                blockBuilder.closeEntry();
                break;
            }
            case StandardTypes.MAP: {
                BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
                for (Entry<?, ?> entry : ((Map<?, ?>) element).entrySet()) {
                    appendToBlockBuilder(type.getTypeParameters().get(0), entry.getKey(), subBlockBuilder);
                    appendToBlockBuilder(type.getTypeParameters().get(1), entry.getValue(), subBlockBuilder);
                }
                blockBuilder.closeEntry();
                break;
            }
            case StandardTypes.BOOLEAN:
                type.writeBoolean(blockBuilder, (Boolean) element);
                break;
            case StandardTypes.TINYINT:
                type.writeLong(blockBuilder, ((Number) element).byteValue());
                break;
            case StandardTypes.SMALLINT:
                type.writeLong(blockBuilder, ((Number) element).shortValue());
                break;
            case StandardTypes.INTEGER:
                type.writeLong(blockBuilder, ((Number) element).intValue());
                break;
            case StandardTypes.BIGINT:
                type.writeLong(blockBuilder, ((Number) element).longValue());
                break;
            case StandardTypes.REAL:
                type.writeLong(blockBuilder, floatToRawIntBits(((Number) element).floatValue()));
                break;
            case StandardTypes.DOUBLE:
                type.writeDouble(blockBuilder, ((Number) element).doubleValue());
                break;
            case StandardTypes.VARCHAR:
                type.writeSlice(blockBuilder, Slices.utf8Slice(element.toString()));
                break;
            case StandardTypes.DATE:
                long daysSinceEpochInLocalZone = ((Date) element).toLocalDate().toEpochDay();
                assertEquals(daysSinceEpochInLocalZone, DateWritable.dateToDays((Date) element));
                type.writeLong(blockBuilder, daysSinceEpochInLocalZone);
                break;
            case StandardTypes.TIMESTAMP:
                Instant instant = ((Timestamp) element).toInstant();
                long epochSecond = instant.getEpochSecond();
                int nano = instant.getNano();
                assertEquals(nano % 1_000_000, 0);
                type.writeLong(blockBuilder, epochSecond * 1000 + nano / 1_000_000);
                break;
            default:
                throw new UnsupportedOperationException("unknown type");
        }
    }
}
