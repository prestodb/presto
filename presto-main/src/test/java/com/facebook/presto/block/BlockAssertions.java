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
package com.facebook.presto.block;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.RowBlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static com.facebook.presto.common.block.ArrayBlock.fromElementBlock;
import static com.facebook.presto.common.block.DictionaryId.randomDictionaryId;
import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.block.RowBlock.fromFieldBlocks;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.common.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.common.type.Decimals.writeBigDecimal;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.testing.TestingEnvironment.getOperatorMethodHandle;
import static com.facebook.presto.type.ColorType.COLOR;
import static com.facebook.presto.util.StructuralTestUtil.appendToBlockBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public final class BlockAssertions
{
    private static final int ENTRY_SIZE = 4;
    private static final int MAX_STRING_SIZE = 50;

    private BlockAssertions()
    {
    }

    public static Object getOnlyValue(Type type, Block block)
    {
        assertEquals(block.getPositionCount(), 1, "Block positions");
        return type.getObjectValue(SESSION.getSqlFunctionProperties(), block, 0);
    }

    public static List<Object> toValues(Type type, Iterable<Block> blocks)
    {
        List<Object> values = new ArrayList<>();
        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                values.add(type.getObjectValue(SESSION.getSqlFunctionProperties(), block, position));
            }
        }
        return Collections.unmodifiableList(values);
    }

    public static List<Object> toValues(Type type, Block block)
    {
        List<Object> values = new ArrayList<>();
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.add(type.getObjectValue(SESSION.getSqlFunctionProperties(), block, position));
        }
        return Collections.unmodifiableList(values);
    }

    public static void assertBlockEquals(Type type, Block actual, Block expected)
    {
        assertEquals(actual.getPositionCount(), expected.getPositionCount());
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION.getSqlFunctionProperties(), actual, position), type.getObjectValue(SESSION.getSqlFunctionProperties(), expected, position));
        }
    }

    public static Block createEmptyBlock(Type type)
    {
        return createAllNullsBlock(type, 0);
    }

    public static Block createAllNullsBlock(Type type, int positionCount)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        for (int i = 0; i < positionCount; i++) {
            blockBuilder.appendNull();
        }
        return blockBuilder.build();
    }

    public static Block createStringsBlock(String... values)
    {
        requireNonNull(values, "varargs 'values' is null");

        return createStringsBlock(Arrays.asList(values));
    }

    public static Block createStringsBlock(Iterable<String> values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, (int) StreamSupport.stream(values.spliterator(), false).count());

        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                VARCHAR.writeString(builder, value);
            }
        }

        return builder.build();
    }

    public static Block createRandomStringBlock(int positionCount, float nullRate, int maxStringLength)
    {
        ValuesWithNullsGenerator<String> generator = new ValuesWithNullsGenerator(nullRate, () -> generateRandomStringWithLength(maxStringLength));

        return createStringsBlock(
                IntStream.range(0, positionCount)
                        .mapToObj(i -> generator.next())
                        .collect(Collectors.toList()));
    }

    public static Block createSlicesBlock(Slice... values)
    {
        requireNonNull(values, "varargs 'values' is null");
        return createSlicesBlock(Arrays.asList(values));
    }

    public static Block createSlicesBlock(Iterable<Slice> values)
    {
        BlockBuilder builder = VARBINARY.createBlockBuilder(null, 100);

        for (Slice value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                VARBINARY.writeSlice(builder, value);
            }
        }

        return builder.build();
    }

    public static Block createStringSequenceBlock(int start, int end)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 100);

        for (int i = start; i < end; i++) {
            VARCHAR.writeString(builder, String.valueOf(i));
        }

        return builder.build();
    }

    public static Block createStringDictionaryBlock(int start, int length)
    {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            VARCHAR.writeString(builder, String.valueOf(i));
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createStringArraysBlock(Iterable<? extends Iterable<String>> values)
    {
        ArrayType arrayType = new ArrayType(VARCHAR);
        BlockBuilder builder = arrayType.createBlockBuilder(null, 100);

        for (Iterable<String> value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                arrayType.writeObject(builder, createStringsBlock(value));
            }
        }

        return builder.build();
    }

    public static <K, V> Block createMapBlock(MapType type, Map<K, V> map)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, map.size());
        for (Map.Entry<K, V> entry : map.entrySet()) {
            BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
            appendToBlockBuilder(BIGINT, entry.getKey(), entryBuilder);
            appendToBlockBuilder(BIGINT, entry.getValue(), entryBuilder);
            blockBuilder.closeEntry();
        }
        return blockBuilder.build();
    }

    public static Block createBooleansBlock(Boolean... values)
    {
        requireNonNull(values, "varargs 'values' is null");

        return createBooleansBlock(Arrays.asList(values));
    }

    public static Block createBooleansBlock(Boolean value, int count)
    {
        return createBooleansBlock(Collections.nCopies(count, value));
    }

    public static Block createBooleansBlock(Iterable<Boolean> values)
    {
        BlockBuilder builder = BOOLEAN.createBlockBuilder(null, 100);

        for (Boolean value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                BOOLEAN.writeBoolean(builder, value);
            }
        }

        return builder.build();
    }

    // Note: Make sure positionCount is sufficiently large if nullRate is greater than 0
    public static Block createRandomBooleansBlock(int positionCount, float nullRate)
    {
        ValuesWithNullsGenerator<Boolean> generator = new ValuesWithNullsGenerator(nullRate, () -> ThreadLocalRandom.current().nextBoolean());

        return createBooleansBlock(
                IntStream.range(0, positionCount)
                        .mapToObj(i -> generator.next())
                        .collect(Collectors.toList()));
    }

    public static Block createShortDecimalsBlock(String... values)
    {
        requireNonNull(values, "varargs 'values' is null");

        return createShortDecimalsBlock(Arrays.asList(values));
    }

    public static Block createShortDecimalsBlock(Iterable<String> values)
    {
        DecimalType shortDecimalType = DecimalType.createDecimalType(1);
        BlockBuilder builder = shortDecimalType.createBlockBuilder(null, 100);

        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                shortDecimalType.writeLong(builder, new BigDecimal(value).unscaledValue().longValue());
            }
        }

        return builder.build();
    }

    // Note: Make sure positionCount is sufficiently large if nullRate is greater than 0
    public static Block createRandomShortDecimalsBlock(int positionCount, float nullRate)
    {
        ValuesWithNullsGenerator<String> generator = new ValuesWithNullsGenerator(nullRate, () -> Double.toString(ThreadLocalRandom.current().nextDouble() * ThreadLocalRandom.current().nextInt()));

        return createShortDecimalsBlock(
                IntStream.range(0, positionCount)
                        .mapToObj(i -> generator.next())
                        .collect(Collectors.toList()));
    }

    public static Block createLongDecimalsBlock(String... values)
    {
        requireNonNull(values, "varargs 'values' is null");

        return createLongDecimalsBlock(Arrays.asList(values));
    }

    public static Block createLongDecimalsBlock(Iterable<String> values)
    {
        DecimalType longDecimalType = DecimalType.createDecimalType(MAX_SHORT_PRECISION + 1);
        BlockBuilder builder = longDecimalType.createBlockBuilder(null, 100);

        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                writeBigDecimal(longDecimalType, builder, new BigDecimal(value));
            }
        }

        return builder.build();
    }

    // Note: Make sure positionCount is sufficiently large if nullRate is greater than 0
    public static Block createRandomLongDecimalsBlock(int positionCount, float nullRate)
    {
        ValuesWithNullsGenerator<String> generator = new ValuesWithNullsGenerator(nullRate, () -> Double.toString(ThreadLocalRandom.current().nextDouble() * ThreadLocalRandom.current().nextInt()));

        return createLongDecimalsBlock(
                IntStream.range(0, positionCount)
                        .mapToObj(i -> generator.next())
                        .collect(Collectors.toList()));
    }

    public static Block createIntsBlock(Integer... values)
    {
        requireNonNull(values, "varargs 'values' is null");

        return createIntsBlock(Arrays.asList(values));
    }

    public static Block createIntsBlock(Iterable<Integer> values)
    {
        BlockBuilder builder = INTEGER.createBlockBuilder(null, 100);

        for (Integer value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                INTEGER.writeLong(builder, value);
            }
        }

        return builder.build();
    }

    public static Block createRowBlock(List<Type> fieldTypes, Object[]... rows)
    {
        BlockBuilder rowBlockBuilder = new RowBlockBuilder(fieldTypes, null, 1);
        for (Object[] row : rows) {
            if (row == null) {
                rowBlockBuilder.appendNull();
                continue;
            }
            BlockBuilder singleRowBlockWriter = rowBlockBuilder.beginBlockEntry();
            for (Object fieldValue : row) {
                if (fieldValue == null) {
                    singleRowBlockWriter.appendNull();
                    continue;
                }

                if (fieldValue instanceof String) {
                    VARCHAR.writeSlice(singleRowBlockWriter, utf8Slice((String) fieldValue));
                }
                else if (fieldValue instanceof Slice) {
                    VARBINARY.writeSlice(singleRowBlockWriter, (Slice) fieldValue);
                }
                else if (fieldValue instanceof Double) {
                    DOUBLE.writeDouble(singleRowBlockWriter, ((Double) fieldValue).doubleValue());
                }
                else if (fieldValue instanceof Long) {
                    BIGINT.writeLong(singleRowBlockWriter, ((Long) fieldValue).longValue());
                }
                else if (fieldValue instanceof Boolean) {
                    BOOLEAN.writeBoolean(singleRowBlockWriter, ((Boolean) fieldValue).booleanValue());
                }
                else if (fieldValue instanceof Block) {
                    singleRowBlockWriter.appendStructure((Block) fieldValue);
                }
                else if (fieldValue instanceof Integer) {
                    INTEGER.writeLong(singleRowBlockWriter, ((Integer) fieldValue).intValue());
                }
                else {
                    throw new IllegalArgumentException();
                }
            }
            rowBlockBuilder.closeEntry();
        }

        return rowBlockBuilder.build();
    }

    // Note: Make sure positionCount is sufficiently large if nullRate is greater than 0
    public static Block createRandomIntsBlock(int positionCount, float nullRate)
    {
        ValuesWithNullsGenerator<Integer> generator = new ValuesWithNullsGenerator(nullRate, () -> ThreadLocalRandom.current().nextInt());

        return createIntsBlock(
                IntStream.range(0, positionCount)
                        .mapToObj(i -> generator.next())
                        .collect(Collectors.toList()));
    }

    public static Block createEmptyLongsBlock()
    {
        return BIGINT.createFixedSizeBlockBuilder(0).build();
    }

    // This method makes it easy to create blocks without having to add an L to every value
    public static Block createLongsBlock(int... values)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, 100);

        for (int value : values) {
            BIGINT.writeLong(builder, (long) value);
        }

        return builder.build();
    }

    public static Block createLongsBlock(Long... values)
    {
        requireNonNull(values, "varargs 'values' is null");

        return createLongsBlock(Arrays.asList(values));
    }

    public static Block createLongsBlock(Iterable<Long> values)
    {
        return createTypedLongsBlock(BIGINT, values);
    }

    // Note: Make sure positionCount is sufficiently large if nullRate is greater than 0
    public static Block createRandomLongsBlock(int positionCount, float nullRate)
    {
        ValuesWithNullsGenerator<Long> generator = new ValuesWithNullsGenerator(nullRate, () -> ThreadLocalRandom.current().nextLong());

        return createLongsBlock(
                IntStream.range(0, positionCount)
                        .mapToObj(i -> generator.next())
                        .collect(Collectors.toList()));
    }

    public static Block createTypedLongsBlock(Type type, Iterable<Long> values)
    {
        BlockBuilder builder = type.createBlockBuilder(null, 100);

        for (Long value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, value);
            }
        }

        return builder.build();
    }

    // Note: Make sure positionCount is sufficiently large if nullRate is greater than 0
    public static Block createRandomSmallintsBlock(int positionCount, float nullRate)
    {
        ValuesWithNullsGenerator<Long> generator = new ValuesWithNullsGenerator(nullRate, () -> ThreadLocalRandom.current().nextLong() % Short.MIN_VALUE);

        return createTypedLongsBlock(
                SMALLINT,
                IntStream.range(0, positionCount)
                        .mapToObj(i -> generator.next())
                        .collect(Collectors.toList()));
    }

    public static Block createLongSequenceBlock(int start, int end)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            BIGINT.writeLong(builder, i);
        }

        return builder.build();
    }

    public static Block createLongDictionaryBlock(int start, int length)
    {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = BIGINT.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            BIGINT.writeLong(builder, i);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createLongRepeatBlock(int value, int length)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(length);
        for (int i = 0; i < length; i++) {
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }

    public static Block createDoubleRepeatBlock(double value, int length)
    {
        BlockBuilder builder = DOUBLE.createFixedSizeBlockBuilder(length);
        for (int i = 0; i < length; i++) {
            DOUBLE.writeDouble(builder, value);
        }
        return builder.build();
    }

    public static Block createTimestampsWithTimezoneBlock(Long... values)
    {
        BlockBuilder builder = TIMESTAMP_WITH_TIME_ZONE.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            TIMESTAMP_WITH_TIME_ZONE.writeLong(builder, value);
        }
        return builder.build();
    }

    public static Block createBooleanSequenceBlock(int start, int end)
    {
        BlockBuilder builder = BOOLEAN.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            BOOLEAN.writeBoolean(builder, i % 2 == 0);
        }

        return builder.build();
    }

    public static Block createBlockOfReals(Float... values)
    {
        requireNonNull(values, "varargs 'values' is null");

        return createBlockOfReals(Arrays.asList(values));
    }

    public static Block createBlockOfReals(Iterable<Float> values)
    {
        BlockBuilder builder = REAL.createBlockBuilder(null, 100);
        for (Float value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                REAL.writeLong(builder, floatToRawIntBits(value));
            }
        }
        return builder.build();
    }

    public static Block createSequenceBlockOfReal(int start, int end)
    {
        BlockBuilder builder = REAL.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            REAL.writeLong(builder, floatToRawIntBits((float) i));
        }

        return builder.build();
    }

    public static Block createDoublesBlock(Double... values)
    {
        requireNonNull(values, "varargs 'values' is null");

        return createDoublesBlock(Arrays.asList(values));
    }

    public static Block createDoublesBlock(Iterable<Double> values)
    {
        BlockBuilder builder = DOUBLE.createBlockBuilder(null, 100);

        for (Double value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                DOUBLE.writeDouble(builder, value);
            }
        }

        return builder.build();
    }

    public static Block createDoubleSequenceBlock(int start, int end)
    {
        BlockBuilder builder = DOUBLE.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            DOUBLE.writeDouble(builder, (double) i);
        }

        return builder.build();
    }

    public static Block createArrayBigintBlock(Iterable<? extends Iterable<Long>> values)
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        BlockBuilder builder = arrayType.createBlockBuilder(null, 100);

        for (Iterable<Long> value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                arrayType.writeObject(builder, createLongsBlock(value));
            }
        }

        return builder.build();
    }

    public static Block createDateSequenceBlock(int start, int end)
    {
        BlockBuilder builder = DATE.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            DATE.writeLong(builder, i);
        }

        return builder.build();
    }

    public static Block createTimestampSequenceBlock(int start, int end)
    {
        BlockBuilder builder = TIMESTAMP.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            TIMESTAMP.writeLong(builder, i);
        }

        return builder.build();
    }

    public static Block createShortDecimalSequenceBlock(int start, int end, DecimalType type)
    {
        BlockBuilder builder = type.createFixedSizeBlockBuilder(end - start);
        long base = BigInteger.TEN.pow(type.getScale()).longValue();

        for (int i = start; i < end; ++i) {
            type.writeLong(builder, base * i);
        }

        return builder.build();
    }

    public static Block createLongDecimalSequenceBlock(int start, int end, DecimalType type)
    {
        BlockBuilder builder = type.createFixedSizeBlockBuilder(end - start);
        BigInteger base = BigInteger.TEN.pow(type.getScale());

        for (int i = start; i < end; ++i) {
            type.writeSlice(builder, encodeUnscaledValue(BigInteger.valueOf(i).multiply(base)));
        }

        return builder.build();
    }

    public static Block createColorRepeatBlock(int value, int length)
    {
        BlockBuilder builder = COLOR.createFixedSizeBlockBuilder(length);
        for (int i = 0; i < length; i++) {
            COLOR.writeLong(builder, value);
        }
        return builder.build();
    }

    public static Block createColorSequenceBlock(int start, int end)
    {
        BlockBuilder builder = COLOR.createBlockBuilder(null, end - start);
        for (int i = start; i < end; ++i) {
            COLOR.writeLong(builder, i);
        }
        return builder.build();
    }

    public static RunLengthEncodedBlock createRLEBlock(double value, int positionCount)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, 1);
        DOUBLE.writeDouble(blockBuilder, value);
        return new RunLengthEncodedBlock(blockBuilder.build(), positionCount);
    }

    public static RunLengthEncodedBlock createRLEBlock(long value, int positionCount)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, 1);
        BIGINT.writeLong(blockBuilder, value);
        return new RunLengthEncodedBlock(blockBuilder.build(), positionCount);
    }

    public static RunLengthEncodedBlock createRLEBlock(String value, int positionCount)
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 1);
        VARCHAR.writeSlice(blockBuilder, wrappedBuffer(value.getBytes()));
        return new RunLengthEncodedBlock(blockBuilder.build(), positionCount);
    }

    public static RunLengthEncodedBlock createRleBlockWithRandomValue(Block block, int positionCount)
    {
        checkArgument(block.getPositionCount() > 0, format("block positions %d is less than or equal to 0", block.getPositionCount()));
        return new RunLengthEncodedBlock(block.getRegion(block.getPositionCount() / 2, 1), positionCount);
    }

    public static DictionaryBlock createRandomDictionaryBlock(Block dictionary, int positionCount)
    {
        return createRandomDictionaryBlock(dictionary, positionCount, false);
    }

    public static DictionaryBlock createRandomDictionaryBlock(Block dictionary, int positionCount, boolean isView)
    {
        checkArgument(dictionary.getPositionCount() > 0, format("dictionary's positionCount %d is less than or equal to 0", dictionary.getPositionCount()));

        int idsOffset = 0;
        if (isView) {
            idsOffset = min(ThreadLocalRandom.current().nextInt(dictionary.getPositionCount()), 1);
        }

        int[] ids = IntStream.range(0, positionCount + idsOffset).map(i -> ThreadLocalRandom.current().nextInt(max(dictionary.getPositionCount() / 10, 1))).toArray();
        return new DictionaryBlock(idsOffset, positionCount, dictionary, ids, false, randomDictionaryId());
    }

    // Note: Make sure positionCount is sufficiently large if nestedNullRate or primitiveNullRate is greater than 0
    public static Block createRandomBlockForType(
            Type type,
            int positionCount,
            float primitiveNullRate,
            float nestedNullRate,
            boolean createView,
            List<Encoding> wrappings)
    {
        verifyNullRate(primitiveNullRate);
        verifyNullRate(nestedNullRate);

        Block block = null;

        if (createView) {
            positionCount *= 2;
        }

        if (type == BOOLEAN) {
            block = createRandomBooleansBlock(positionCount, primitiveNullRate);
        }
        else if (type == BIGINT) {
            block = createRandomLongsBlock(positionCount, primitiveNullRate);
        }
        else if (type == INTEGER || type == REAL) {
            block = createRandomIntsBlock(positionCount, primitiveNullRate);
        }
        else if (type == SMALLINT) {
            block = createRandomSmallintsBlock(positionCount, primitiveNullRate);
        }
        else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                block = createRandomLongsBlock(positionCount, primitiveNullRate);
            }
            else {
                block = createRandomLongDecimalsBlock(positionCount, primitiveNullRate);
            }
        }
        else if (type == VARCHAR) {
            block = createRandomStringBlock(positionCount, primitiveNullRate, MAX_STRING_SIZE);
        }
        else {
            // Nested types
            // Build isNull and offsets of size positionCount
            boolean[] isNull = null;
            if (nestedNullRate > 0) {
                isNull = new boolean[positionCount];
            }
            int[] offsets = new int[positionCount + 1];

            for (int position = 0; position < positionCount; position++) {
                if (nestedNullRate > 0 && ThreadLocalRandom.current().nextDouble(1) < nestedNullRate) {
                    isNull[position] = true;
                    offsets[position + 1] = offsets[position];
                }
                else {
                    offsets[position + 1] = offsets[position] + (type instanceof RowType ? 1 : ThreadLocalRandom.current().nextInt(ENTRY_SIZE) + 1);
                }
            }

            // Build the nested block of size offsets[positionCount].
            if (type instanceof ArrayType) {
                Block valuesBlock = createRandomBlockForType(((ArrayType) type).getElementType(), offsets[positionCount], primitiveNullRate, nestedNullRate, createView, wrappings);
                block = fromElementBlock(positionCount, Optional.ofNullable(isNull), offsets, valuesBlock);
            }
            else if (type instanceof MapType) {
                MapType mapType = (MapType) type;
                Block keyBlock = createRandomBlockForType(mapType.getKeyType(), offsets[positionCount], 0.0f, 0.0f, createView, wrappings);
                Block valueBlock = createRandomBlockForType(mapType.getValueType(), offsets[positionCount], primitiveNullRate, nestedNullRate, createView, wrappings);

                block = mapType.createBlockFromKeyValue(positionCount, Optional.ofNullable(isNull), offsets, keyBlock, valueBlock);
            }
            else if (type instanceof RowType) {
                List<Type> fieldTypes = type.getTypeParameters();
                Block[] fieldBlocks = new Block[fieldTypes.size()];

                for (int i = 0; i < fieldBlocks.length; i++) {
                    fieldBlocks[i] = createRandomBlockForType(fieldTypes.get(i), positionCount, primitiveNullRate, nestedNullRate, createView, wrappings);
                }

                block = fromFieldBlocks(positionCount, Optional.ofNullable(isNull), fieldBlocks);
            }
            else {
                throw new IllegalArgumentException(format("type %s is not supported.", type));
            }
        }

        if (createView) {
            positionCount /= 2;
            int offset = positionCount / 2;
            block = block.getRegion(offset, positionCount);
        }

        if (!wrappings.isEmpty()) {
            block = wrapBlock(block, positionCount, wrappings);
        }

        return block;
    }

    public static Block wrapBlock(Block block, int positionCount, List<Encoding> wrappings)
    {
        checkArgument(!wrappings.isEmpty(), "wrappings is empty");

        Block wrappedBlock = block;

        for (int i = wrappings.size() - 1; i >= 0; i--) {
            switch (wrappings.get(i)) {
                case DICTIONARY:
                    wrappedBlock = createRandomDictionaryBlock(wrappedBlock, positionCount, true);
                    break;
                case RUN_LENGTH:
                    wrappedBlock = createRleBlockWithRandomValue(wrappedBlock, positionCount);
                    break;
                default:
                    throw new IllegalArgumentException(format("wrappings %s is incorrect", wrappings));
            }
        }
        return wrappedBlock;
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

    public enum Encoding
    {
        DICTIONARY,
        RUN_LENGTH
    }

    private static String generateRandomStringWithLength(int length)
    {
        byte[] array = new byte[length];
        ThreadLocalRandom.current().nextBytes(array);
        return new String(array, UTF_8);
    }

    private static final class ValuesWithNullsGenerator<T>
    {
        private final float nullRate;
        private final Supplier<T> supplier;

        private ValuesWithNullsGenerator(float nullRate, Supplier<T> supplier)
        {
            verifyNullRate(nullRate);

            this.nullRate = nullRate;
            this.supplier = requireNonNull(supplier, "supplier is null");
        }

        public T next()
        {
            return nullRate > 0 && ThreadLocalRandom.current().nextDouble(1) < nullRate ? null : supplier.get();
        }
    }

    private static void verifyNullRate(float nullRate)
    {
        if (nullRate < 0 || nullRate > 1) {
            throw new IllegalArgumentException(format("nullRate %f is not valid.", nullRate));
        }
    }
}
