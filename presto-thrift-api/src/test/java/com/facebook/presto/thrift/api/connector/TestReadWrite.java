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
package com.facebook.presto.thrift.api.connector;

import com.facebook.airlift.stats.cardinality.HyperLogLog;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.thrift.api.datatypes.PrestoThriftBlock;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.thrift.api.connector.PrestoThriftPageResult.fromRecordSet;
import static com.facebook.presto.thrift.api.datatypes.PrestoThriftBlock.fromBlock;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestReadWrite
{
    private static final double NULL_FRACTION = 0.1;
    private static final int MAX_VARCHAR_GENERATED_LENGTH = 64;
    private static final char[] SYMBOLS;
    private static final long MIN_GENERATED_TIMESTAMP;
    private static final long MAX_GENERATED_TIMESTAMP;
    private static final int MIN_GENERATED_DATE;
    private static final int MAX_GENERATED_DATE;
    private static final int MAX_GENERATED_JSON_KEY_LENGTH = 8;
    private static final int HYPER_LOG_LOG_BUCKETS = 128;
    private static final int MAX_HYPER_LOG_LOG_ELEMENTS = 32;
    private static final int MAX_ARRAY_GENERATED_LENGTH = 64;
    private final AtomicLong singleRowPageSeedGenerator = new AtomicLong(762103512L);
    private final AtomicLong singleRowRecordSetSeedGenerator = new AtomicLong(762103512L);
    private final AtomicLong multiRowPageSeedGenerator = new AtomicLong(762103512L);
    private final AtomicLong multiRowRecordSetSeedGenerator = new AtomicLong(762103512L);
    private final List<ColumnDefinition> columns = ImmutableList.of(
            new IntegerColumn(),
            new BigintColumn(),
            new DoubleColumn(),
            new VarcharColumn(createUnboundedVarcharType()),
            new VarcharColumn(createVarcharType(MAX_VARCHAR_GENERATED_LENGTH / 2)),
            new BooleanColumn(),
            new DateColumn(),
            new TimestampColumn(),
            new JsonColumn(),
            new HyperLogLogColumn(),
            new BigintArrayColumn());

    static {
        char[] symbols = new char[2 * 26 + 10];
        int next = 0;
        for (char ch = 'A'; ch <= 'Z'; ch++) {
            symbols[next++] = ch;
        }
        for (char ch = 'a'; ch <= 'z'; ch++) {
            symbols[next++] = ch;
        }
        for (char ch = '0'; ch <= '9'; ch++) {
            symbols[next++] = ch;
        }
        SYMBOLS = symbols;

        Calendar calendar = Calendar.getInstance();

        calendar.set(2000, Calendar.JANUARY, 1);
        MIN_GENERATED_TIMESTAMP = calendar.getTimeInMillis();
        MIN_GENERATED_DATE = toIntExact(MILLISECONDS.toDays(MIN_GENERATED_TIMESTAMP));

        calendar.set(2020, Calendar.DECEMBER, 31);
        MAX_GENERATED_TIMESTAMP = calendar.getTimeInMillis();
        MAX_GENERATED_DATE = toIntExact(MILLISECONDS.toDays(MAX_GENERATED_TIMESTAMP));
    }

    @Test(invocationCount = 20)
    public void testSingleRowPageReadWrite()
    {
        testPageReadWrite(new Random(singleRowPageSeedGenerator.incrementAndGet()), 1);
    }

    @Test(invocationCount = 20)
    public void testSingleRowRecordSetReadWrite()
    {
        testRecordSetReadWrite(new Random(singleRowRecordSetSeedGenerator.incrementAndGet()), 1);
    }

    @Test(invocationCount = 20)
    public void testMultiRowPageReadWrite()
    {
        Random random = new Random(multiRowPageSeedGenerator.incrementAndGet());
        testPageReadWrite(random, random.nextInt(10000) + 10000);
    }

    @Test(invocationCount = 20)
    public void testMultiRowRecordSetReadWrite()
    {
        Random random = new Random(multiRowRecordSetSeedGenerator.incrementAndGet());
        testRecordSetReadWrite(random, random.nextInt(10000) + 10000);
    }

    private void testPageReadWrite(Random random, int records)
    {
        testReadWrite(random, records, blocks -> {
            List<PrestoThriftBlock> columnBlocks = new ArrayList<>(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                columnBlocks.add(fromBlock(blocks.get(i), columns.get(i).getType()));
            }
            return new PrestoThriftPageResult(columnBlocks, records, null);
        });
    }

    private void testRecordSetReadWrite(Random random, int records)
    {
        testReadWrite(random, records, blocks -> {
            List<Type> types = columns.stream().map(ColumnDefinition::getType).collect(toImmutableList());
            ImmutableList.Builder<List<Object>> recordSet = ImmutableList.builder();
            for (int i = 0; i < records; i++) {
                List<Object> record = new ArrayList<>();
                for (int j = 0; j < types.size(); j++) {
                    record.add(readNativeValue(types.get(j), blocks.get(j), i));
                }
                recordSet.add(record);
            }
            InMemoryRecordSet inputRecordSet = new InMemoryRecordSet(types, recordSet.build());
            return fromRecordSet(inputRecordSet);
        });
    }

    private void testReadWrite(Random random, int records, Function<List<Block>, PrestoThriftPageResult> convert)
    {
        // generate columns data
        List<Block> inputBlocks = new ArrayList<>(columns.size());
        for (ColumnDefinition column : columns) {
            inputBlocks.add(generateColumn(column, random, records));
        }

        // convert column data to thrift ("write step")
        PrestoThriftPageResult batch = convert.apply(inputBlocks);

        // convert thrift data to page/blocks ("read step")
        Page page = batch.toPage(columns.stream().map(ColumnDefinition::getType).collect(toImmutableList()));

        // compare the result with original input
        assertNotNull(page);
        assertEquals(page.getChannelCount(), columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Block actual = page.getBlock(i);
            Block expected = inputBlocks.get(i);
            assertBlock(actual, expected, columns.get(i));
        }
    }

    private static Block generateColumn(ColumnDefinition column, Random random, int records)
    {
        BlockBuilder builder = column.getType().createBlockBuilder(null, records);
        for (int i = 0; i < records; i++) {
            if (random.nextDouble() < NULL_FRACTION) {
                builder.appendNull();
            }
            else {
                column.writeNextRandomValue(random, builder);
            }
        }
        return builder.build();
    }

    private static void assertBlock(Block actual, Block expected, ColumnDefinition columnDefinition)
    {
        assertEquals(actual.getPositionCount(), expected.getPositionCount());
        int positions = actual.getPositionCount();
        for (int i = 0; i < positions; i++) {
            Object actualValue = columnDefinition.extractValue(actual, i);
            Object expectedValue = columnDefinition.extractValue(expected, i);
            assertEquals(actualValue, expectedValue);
        }
    }

    private static String nextString(Random random)
    {
        return nextString(random, MAX_VARCHAR_GENERATED_LENGTH);
    }

    private static String nextString(Random random, int maxLength)
    {
        int size = random.nextInt(maxLength);
        char[] result = new char[size];
        for (int i = 0; i < size; i++) {
            result[i] = SYMBOLS[random.nextInt(SYMBOLS.length)];
        }
        return new String(result);
    }

    private static long nextTimestamp(Random random)
    {
        return MIN_GENERATED_TIMESTAMP + (long) (random.nextDouble() * (MAX_GENERATED_TIMESTAMP - MIN_GENERATED_TIMESTAMP));
    }

    private static int nextDate(Random random)
    {
        return MIN_GENERATED_DATE + random.nextInt(MAX_GENERATED_DATE - MIN_GENERATED_DATE);
    }

    private static Slice nextHyperLogLog(Random random)
    {
        HyperLogLog hll = HyperLogLog.newInstance(HYPER_LOG_LOG_BUCKETS);
        int size = random.nextInt(MAX_HYPER_LOG_LOG_ELEMENTS);
        for (int i = 0; i < size; i++) {
            hll.add(random.nextLong());
        }
        return hll.serialize();
    }

    private static void generateBigintArray(Random random, BlockBuilder parentBuilder)
    {
        int numberOfElements = random.nextInt(MAX_ARRAY_GENERATED_LENGTH);
        BlockBuilder builder = parentBuilder.beginBlockEntry();
        for (int i = 0; i < numberOfElements; i++) {
            if (random.nextDouble() < NULL_FRACTION) {
                builder.appendNull();
            }
            else {
                builder.writeLong(random.nextLong());
            }
        }
        parentBuilder.closeEntry();
    }

    private abstract static class ColumnDefinition
    {
        private final Type type;

        public ColumnDefinition(Type type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        public Type getType()
        {
            return type;
        }

        abstract Object extractValue(Block block, int position);

        abstract void writeNextRandomValue(Random random, BlockBuilder builder);
    }

    private static final class IntegerColumn
            extends ColumnDefinition
    {
        public IntegerColumn()
        {
            super(INTEGER);
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return INTEGER.getLong(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            INTEGER.writeLong(builder, random.nextInt());
        }
    }

    private static final class BigintColumn
            extends ColumnDefinition
    {
        public BigintColumn()
        {
            super(BIGINT);
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return BIGINT.getLong(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            BIGINT.writeLong(builder, random.nextLong());
        }
    }

    private static final class DoubleColumn
            extends ColumnDefinition
    {
        public DoubleColumn()
        {
            super(DOUBLE);
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return DOUBLE.getDouble(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            DOUBLE.writeDouble(builder, random.nextDouble());
        }
    }

    private static final class VarcharColumn
            extends ColumnDefinition
    {
        private final VarcharType varcharType;

        public VarcharColumn(VarcharType varcharType)
        {
            super(varcharType);
            this.varcharType = requireNonNull(varcharType, "varcharType is null");
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return varcharType.getSlice(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            varcharType.writeString(builder, nextString(random));
        }
    }

    private static final class BooleanColumn
            extends ColumnDefinition
    {
        public BooleanColumn()
        {
            super(BOOLEAN);
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return BOOLEAN.getBoolean(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            BOOLEAN.writeBoolean(builder, random.nextBoolean());
        }
    }

    private static final class DateColumn
            extends ColumnDefinition
    {
        public DateColumn()
        {
            super(DATE);
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return DATE.getLong(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            DATE.writeLong(builder, nextDate(random));
        }
    }

    private static final class TimestampColumn
            extends ColumnDefinition
    {
        public TimestampColumn()
        {
            super(TIMESTAMP);
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return TIMESTAMP.getLong(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            TIMESTAMP.writeLong(builder, nextTimestamp(random));
        }
    }

    private static final class JsonColumn
            extends ColumnDefinition
    {
        public JsonColumn()
        {
            super(JSON);
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return JSON.getSlice(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            String json = String.format("{\"%s\": %d, \"%s\": \"%s\"}",
                    nextString(random, MAX_GENERATED_JSON_KEY_LENGTH),
                    random.nextInt(),
                    nextString(random, MAX_GENERATED_JSON_KEY_LENGTH),
                    random.nextInt());
            JSON.writeString(builder, json);
        }
    }

    private static final class HyperLogLogColumn
            extends ColumnDefinition
    {
        public HyperLogLogColumn()
        {
            super(HYPER_LOG_LOG);
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return HYPER_LOG_LOG.getSlice(block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            HYPER_LOG_LOG.writeSlice(builder, nextHyperLogLog(random));
        }
    }

    private static final class BigintArrayColumn
            extends ColumnDefinition
    {
        private final ArrayType arrayType;

        public BigintArrayColumn()
        {
            this(new ArrayType(BIGINT));
        }

        private BigintArrayColumn(ArrayType arrayType)
        {
            super(arrayType);
            this.arrayType = requireNonNull(arrayType, "arrayType is null");
        }

        @Override
        Object extractValue(Block block, int position)
        {
            return arrayType.getObjectValue(null, block, position);
        }

        @Override
        void writeNextRandomValue(Random random, BlockBuilder builder)
        {
            generateBigintArray(random, builder);
        }
    }
}
