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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.statistics.IntegerStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.StringStatisticsBuilder;
import com.facebook.presto.orc.writer.ColumnWriter;
import com.facebook.presto.orc.writer.DictionaryColumnWriter;
import com.facebook.presto.orc.writer.LongColumnWriter;
import com.facebook.presto.orc.writer.LongDictionaryColumnWriter;
import com.facebook.presto.orc.writer.SliceDictionaryColumnWriter;
import com.facebook.presto.orc.writer.SliceDirectColumnWriter;
import io.airlift.units.DataSize;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcWriterOptions.DEFAULT_MAX_STRING_STATISTICS_LIMIT;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkDictionaryWriter
{
    private static final int COLUMN_INDEX = 0;
    private static final int STRING_LIMIT_BYTES = toIntExact(DEFAULT_MAX_STRING_STATISTICS_LIMIT.toBytes());

    private ColumnWriterOptions getColumnWriterOptions()
    {
        return getColumnWriterOptions(true);
    }

    private ColumnWriterOptions getColumnWriterOptions(boolean sortStringDictionaryKeys)
    {
        return ColumnWriterOptions.builder()
                .setCompressionKind(CompressionKind.NONE)
                .setStringDictionarySortingEnabled(sortStringDictionaryKeys)
                .build();
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkDictionaryWriter.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }

    private StringStatisticsBuilder newStringStatisticsBuilder()
    {
        return new StringStatisticsBuilder(STRING_LIMIT_BYTES);
    }

    @Benchmark
    public void writeDirect(BenchmarkData data)
    {
        ColumnWriter columnWriter;
        Type type = data.getType();
        if (type.equals(VARCHAR)) {
            columnWriter = new SliceDirectColumnWriter(COLUMN_INDEX, type, getColumnWriterOptions(), Optional.empty(), DWRF, this::newStringStatisticsBuilder, DWRF.createMetadataWriter());
        }
        else {
            columnWriter = new LongColumnWriter(COLUMN_INDEX, type, getColumnWriterOptions(), Optional.empty(), DWRF, IntegerStatisticsBuilder::new, DWRF.createMetadataWriter());
        }
        for (Block block : data.getBlocks()) {
            columnWriter.beginRowGroup();
            columnWriter.writeBlock(block);
            columnWriter.finishRowGroup();
        }
        columnWriter.close();
        columnWriter.reset();
    }

    private void writeDictionary(BenchmarkData data, boolean sortStringDictionaryKeys)
    {
        ColumnWriter columnWriter = getDictionaryColumnWriter(data, sortStringDictionaryKeys);
        for (Block block : data.getBlocks()) {
            columnWriter.beginRowGroup();
            columnWriter.writeBlock(block);
            columnWriter.finishRowGroup();
        }
        columnWriter.close();
        columnWriter.reset();
    }

    @Benchmark
    public void writeDictionary(BenchmarkData data)
    {
        writeDictionary(data, true);
    }

    @Benchmark
    public void writeDictionaryAndConvert(BenchmarkData data)
    {
        DictionaryColumnWriter columnWriter = getDictionaryColumnWriter(data, true);
        for (Block block : data.getBlocks()) {
            columnWriter.beginRowGroup();
            columnWriter.writeBlock(block);
            columnWriter.finishRowGroup();
        }
        int maxDirectBytes = toIntExact(new DataSize(512, MEGABYTE).toBytes());
        OptionalInt optionalInt = columnWriter.tryConvertToDirect(maxDirectBytes);
        checkState(optionalInt.isPresent(), "Column did not covert to direct");
        columnWriter.close();
        columnWriter.reset();
    }

    @Benchmark
    public void writeDictionaryNoSorting(BenchmarkData data)
    {
        writeDictionary(data, false);
    }

    private DictionaryColumnWriter getDictionaryColumnWriter(BenchmarkData data, boolean sortStringDictionaryKeys)
    {
        DictionaryColumnWriter columnWriter;
        Type type = data.getType();
        ColumnWriterOptions columnWriterOptions = getColumnWriterOptions(sortStringDictionaryKeys);
        if (type.equals(VARCHAR)) {
            columnWriter = new SliceDictionaryColumnWriter(COLUMN_INDEX, type, columnWriterOptions, Optional.empty(), DWRF, DWRF.createMetadataWriter());
        }
        else {
            columnWriter = new LongDictionaryColumnWriter(COLUMN_INDEX, type, columnWriterOptions, Optional.empty(), DWRF, DWRF.createMetadataWriter());
        }
        return columnWriter;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int NUM_BLOCKS = 1_000;
        private static final int ROWS_PER_BLOCK = 10_000;
        private static final String INTEGER_TYPE = "integer";
        private static final String BIGINT_TYPE = "bigint";
        private static final String VARCHAR_TYPE = "varchar";
        private static final String POSSIBLE_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 +-_*#";
        private static final int MAX_STRING_LENGTH = 30;
        private static final int MIN_STRING_LENGTH = 10;

        private final Random random = new Random(0);
        private final List<Block> blocks;

        @Param({
                INTEGER_TYPE,
                BIGINT_TYPE,
                VARCHAR_TYPE
        })
        private String typeSignature = INTEGER_TYPE;

        @Param({
                "1",
                "5",
                "10",
                "100"
        })
        private String uniqueValuesPercentage = "100";
        private Type type;

        public BenchmarkData()
        {
            blocks = new ArrayList<>();
        }

        public List<Block> getBlocks()
        {
            return blocks;
        }

        public Type getType()
        {
            return type;
        }

        @Setup
        public void setUp()
        {
            type = getType(typeSignature);
            for (int i = 0; i < NUM_BLOCKS; i++) {
                blocks.add(getBlock(ROWS_PER_BLOCK));
            }
        }

        private Type getType(String typeSignature)
        {
            switch (typeSignature) {
                case VARCHAR_TYPE:
                    return VARCHAR;
                case BIGINT_TYPE:
                    return BIGINT;
                case INTEGER_TYPE:
                    return INTEGER;
                default:
                    throw new UnsupportedOperationException("Unsupported type " + typeSignature);
            }
        }

        private int getUniqueValues(int numRows)
        {
            int value = Integer.parseInt(uniqueValuesPercentage);
            checkState(value <= 100);
            int uniqueValues = (int) (value * numRows / 100.0);
            return max(uniqueValues, 1);
        }

        private String getNextString(char[] chars, int length)
        {
            for (int i = 0; i < length; i++) {
                chars[i] = POSSIBLE_CHARS.charAt(random.nextInt(POSSIBLE_CHARS.length()));
            }
            return String.valueOf(chars);
        }

        private List<String> generateStrings(int numRows)
        {
            int valuesToGenerate = getUniqueValues(numRows);
            List<String> strings = new ArrayList<>(numRows);
            char[] chars = new char[MAX_STRING_LENGTH];
            for (int i = 0; i < valuesToGenerate; i++) {
                int length = MIN_STRING_LENGTH + random.nextInt(MAX_STRING_LENGTH - MIN_STRING_LENGTH);
                strings.add(getNextString(chars, length));
            }

            for (int i = valuesToGenerate; i < numRows; i++) {
                int randomIndex = random.nextInt(valuesToGenerate);
                strings.add(strings.get(randomIndex));
            }
            return strings;
        }

        private List<Integer> generateIntegers(int numRows)
        {
            int valuesToGenerate = getUniqueValues(numRows);
            List<Integer> integers = new ArrayList<>(numRows);
            for (int i = 0; i < valuesToGenerate; i++) {
                integers.add(random.nextInt());
            }

            for (int i = valuesToGenerate; i < numRows; i++) {
                int randomIndex = random.nextInt(valuesToGenerate);
                integers.add(integers.get(randomIndex));
            }
            return integers;
        }

        private List<Long> generateLongs(int numRows)
        {
            int valuesToGenerate = getUniqueValues(numRows);
            List<Long> longs = new ArrayList<>(numRows);
            for (int i = 0; i < valuesToGenerate; i++) {
                longs.add(random.nextLong());
            }

            for (int i = valuesToGenerate; i < numRows; i++) {
                int randomIndex = random.nextInt(valuesToGenerate);
                longs.add(longs.get(randomIndex));
            }
            return longs;
        }

        private Block getBlock(int numRows)
        {
            BlockBuilder blockBuilder;
            if (type.equals(VARCHAR)) {
                blockBuilder = VARCHAR.createBlockBuilder(null, numRows);
                for (String string : generateStrings(numRows)) {
                    VARCHAR.writeSlice(blockBuilder, utf8Slice(string));
                }
            }
            else if (type.equals(BIGINT)) {
                blockBuilder = BIGINT.createBlockBuilder(null, numRows);
                for (Long value : generateLongs(numRows)) {
                    BIGINT.writeLong(blockBuilder, value);
                }
            }
            else if (type.equals(INTEGER)) {
                blockBuilder = INTEGER.createBlockBuilder(null, numRows);
                for (Integer value : generateIntegers(numRows)) {
                    INTEGER.writeLong(blockBuilder, value.longValue());
                }
            }
            else {
                throw new UnsupportedOperationException("Unsupported type " + typeSignature);
            }
            return blockBuilder.build();
        }
    }
}
