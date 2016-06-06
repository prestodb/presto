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
package com.facebook.presto.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.XxHash64;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;
import static org.testng.Assert.assertEquals;

/*
    Benchmark                                             (dataDistribution)  Mode  Cnt    Score   Error  Units
    BenchmarkJoinWithNativeArrays.benchmarkBuildByteHash              random  avgt   14   52.221 ± 0.765  ms/op
    BenchmarkJoinWithNativeArrays.benchmarkBuildByteHash       unique_random  avgt   14   31.031 ± 0.211  ms/op

    BenchmarkJoinWithNativeArrays.benchmarkBuildHash                  random  avgt   14   55.241 ± 0.391  ms/op
    BenchmarkJoinWithNativeArrays.benchmarkBuildHash           unique_random  avgt   14   54.599 ± 1.586  ms/op

    BenchmarkJoinWithNativeArrays.benchmarkJoinByteHash               random  avgt   14   75.234 ± 1.299  ms/op
    BenchmarkJoinWithNativeArrays.benchmarkJoinByteHash        unique_random  avgt   14   77.927 ± 0.992  ms/op

    BenchmarkJoinWithNativeArrays.benchmarkJoinHash                   random  avgt   14   94.984 ± 1.678  ms/op
    BenchmarkJoinWithNativeArrays.benchmarkJoinHash            unique_random  avgt   14  106.018 ± 1.290  ms/op

    Summary:
    unique
                            no hack           byte hack
    original                115 + 272 = 387   40 + 198 = 238
    original rebase         92 + 175 = 267    37 + 135 = 172
    original no getBlocks   90 + 171 = 261    39 + 125 = 164
    original with inline    90 + 150 = 240    39 + 103 = 142
    original inline rebase  75 + 147 = 222    35 + 103 = 138
    blocks                  80 + 149 = 229    36 + 102 = 138
    native arrays           54 + 106 = 154    31 + 77  = 108

    non unique
                   no hack           byte hack
    original       110 + 202 = 312   81 + 163 = 244
    blocks         77 + 112 = 189    59 + 82  = 141
    native arrays  55 + 94  = 149    52 + 75  = 127
 */
@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 7)
@Measurement(iterations = 7)
public class BenchmarkJoinWithNativeArrays
{
    private static final int POSITIONS_PER_BLOCK = 1024;

    private static class LongColumnBuilder
    {
        private ImmutableList.Builder<Long> builder = ImmutableList.builder();
        private int positionsInBlock = 0;
        private final ImmutableList.Builder<LongBlock> blocks = ImmutableList.builder();

        public LongColumnBuilder add(long value)
        {
            if (positionsInBlock >= POSITIONS_PER_BLOCK) {
                finishBlock();
            }
            positionsInBlock++;
            builder.add(value);

            return this;
        }

        public LongColumn build()
        {
            if (positionsInBlock > 0) {
                finishBlock();
            }
            return new LongColumn(blocks.build().toArray(new LongBlock[0]));
        }

        private void finishBlock()
        {
            List<Long> values = builder.build();
            long[] data = new long[values.size()];
            for (int i = 0; i < values.size(); i++) {
                data[i] = values.get(i);
            }
            blocks.add(new LongBlock(data));
            builder = ImmutableList.builder();
            positionsInBlock = 0;
        }
    }

    private static class LongBlock
    {
        private final long[] data;

        public LongBlock(long[] data)
        {
            this.data = data;
        }

        public long hash(int position)
        {
            return data[position];
        }

        public long get(int position)
        {
            return data[position];
        }

        public int size()
        {
            return data.length;
        }
    }

    private static class LongColumn
    {
        public final LongBlock[] blocks;

        public LongColumn(LongBlock[] blocks)
        {
            this.blocks = blocks;
        }

        public long get(int block, int position)
        {
            return blocks[block].get(position);
        }

        public long hash(int blockIndex, int position)
        {
            return blocks[blockIndex].hash(position);
        }

        public LongBlock getBlock(int block)
        {
            return blocks[block];
        }
    }

    private static class Table
    {
        public final LongColumn col1;
        public final LongColumn col2;

        public Table(LongColumn column1, LongColumn column2)
        {
            col1 = column1;
            col2 = column2;
        }
    }

    @State(Thread)
    public static class BuildContext
    {
        @Param({"random", "unique_random"})
        protected String dataDistribution = "unique_random";

        protected static final int BUILD_ROWS_NUMBER = 700_000;

        public Table buildTable;

        @Setup
        public void setup()
        {
            initializeBuildTable();
        }

        protected void initializeBuildTable()
        {
            Random random = new Random();

            LongColumnBuilder column1 = new LongColumnBuilder();
            LongColumnBuilder column2 = new LongColumnBuilder();
            List<Integer> possibleValues = new ArrayList<>();
            for (int position = 0; position < BUILD_ROWS_NUMBER; position++) {
                possibleValues.add(position);
            }
            Collections.shuffle(possibleValues);
            Iterator<Integer> shuffledValues = possibleValues.iterator();

            for (int position = 0; position < BUILD_ROWS_NUMBER; position++) {
                int val1;
                int val2;
                switch (dataDistribution) {
                    case "sequential":
                        val1 = position;
                        val2 = val1 + 10;
                        break;
                    case "random":
                        val1 = random.nextInt(BUILD_ROWS_NUMBER);
                        val2 = val1 + 10;
                        break;
                    case "unique_random":
                        val1 = shuffledValues.next();
                        val2 = val1 + 10;
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }

                column1.add(val1);
                column2.add(val2);
            }

            buildTable = new Table(column1.build(), column2.build());
        }
    }

    @State(Thread)
    public static class JoinContext extends BuildContext
    {
        protected static final int PROBE_ROWS_NUMBER = 700_000;

        //@Param({"0.1", "1", "2"})
        protected double matchRate = 1;

        public Table probeTable;
        private InMemoryJoinHash joinHash;
        private InMemoryJoinHash joinByteHash;

        @Setup
        public void setup()
        {
            super.setup();
            initializeProbeTable();
        }

        protected void initializeProbeTable()
        {
            LongColumnBuilder column1 = new LongColumnBuilder();
            LongColumnBuilder column2 = new LongColumnBuilder();

            Random random = new Random(42);
            int remainingRows = PROBE_ROWS_NUMBER;
            List<Integer> possibleValues = new ArrayList<>();
            for (int position = 0; position < BUILD_ROWS_NUMBER; position++) {
                possibleValues.add(position);
            }
            Collections.shuffle(possibleValues);
            Iterator<Integer> shuffledValues = possibleValues.iterator();

            while (remainingRows > 0) {
                double roll = random.nextDouble();

                int value = shuffledValues.next();
                int columnA = value;
                int columnB = 10 + value;

                int rowsCount = 1;
                if (matchRate < 1) {
                    // each row has matchRate chance to join
                    if (roll > matchRate) {
                        // generate not matched row
                        columnA *= -1;
                        columnB *= -1;
                    }
                }
                else if (matchRate > 1) {
                    // each row has will be repeated between one and 2*matchRate times
                    roll = roll * 2 * matchRate + 1;
                    // example for matchRate == 2:
                    // roll is within [0, 5) range
                    // rowsCount is within [0, 4] range, where each value has same probability
                    // so expected rowsCount is 2
                    rowsCount = (int) Math.floor(roll);
                }

                for (int i = 0; i < rowsCount; i++) {
                    column1.add(columnA);
                    column2.add(columnB);
                    --remainingRows;
                }
            }

            probeTable = new Table(column1.build(), column2.build());
            joinHash = benchmarkBuildHash(this);
            joinByteHash = benchmarkBuildByteHash(this);
        }

        public InMemoryJoinHash getJoinHash()
        {
            return joinHash;
        }

        public InMemoryJoinHash getJoinByteHash()
        {
            return joinByteHash;
        }
    }

    public void test(InMemoryJoinHash joinHash)
    {
        LongColumnBuilder probeBlockBuilder = new LongColumnBuilder();
        probeBlockBuilder.add(1);
        probeBlockBuilder.add(2);
        probeBlockBuilder.add(42);
        LongColumn probeColumn = probeBlockBuilder.build();

        assertEquals(joinHash.getJoinPosition(0, probeColumn.getBlock(0)), 1);
        assertEquals(joinHash.getNextJoinPosition(1), -1);

        assertEquals(joinHash.getJoinPosition(1, probeColumn.getBlock(0)), -1);

        assertEquals(joinHash.getJoinPosition(2, probeColumn.getBlock(0)), 3);
        assertEquals(joinHash.getNextJoinPosition(3), 2);
        assertEquals(joinHash.getNextJoinPosition(2), -1);
    }

    private static Table buildTable;

    static {
        LongColumnBuilder column1 = new LongColumnBuilder();
        LongColumnBuilder column2 = new LongColumnBuilder();

        column1.add(0).add(1).add(42).add(42).add(4);
        column2.add(0).add(0).add(0).add(0).add(0);

        buildTable = new Table(column1.build(), column2.build());
    }

    @Test
    public void testOriginal()
    {
        BuildContext context = new BuildContext();
        context.buildTable = buildTable;

        InMemoryJoinHash joinHash = benchmarkBuildHash(context);
        test(joinHash);
    }

    @Test
    public void testByte()
    {
        BuildContext context = new BuildContext();
        context.buildTable = buildTable;

        InMemoryJoinHash joinHash = benchmarkBuildByteHash(context);
        test(joinHash);
    }

    @Benchmark
    public static InMemoryJoinHash benchmarkBuildHash(BuildContext buildContext)
    {
        LongArrayList addresses = new LongArrayList();

        int blockIndex = 0;
        for (LongBlock block : buildContext.buildTable.col1.blocks) {
            for (int blockPosition = 0; blockPosition < block.size(); blockPosition++) {
                addresses.add(SyntheticAddress.encodeSyntheticAddress(blockIndex, blockPosition));
            }
            blockIndex++;
        }

        PagesHashStrategy pagesHashStrategy = new PagesHashStrategy(buildContext.buildTable);

        return new InMemoryJoinHash(addresses, pagesHashStrategy).build();
    }

    @Benchmark
    public static InMemoryJoinHash benchmarkBuildByteHash(BuildContext buildContext)
    {
        LongArrayList addresses = new LongArrayList();

        int blockIndex = 0;
        for (LongBlock block : buildContext.buildTable.col1.blocks) {
            for (int blockPosition = 0; blockPosition < block.size(); blockPosition++) {
                addresses.add(SyntheticAddress.encodeSyntheticAddress(blockIndex, blockPosition));
            }
            blockIndex++;
        }

        PagesHashStrategy pagesHashStrategy = new PagesHashStrategy(buildContext.buildTable);

        return new InMemoryJoinHashByte(addresses, pagesHashStrategy).build();
    }

    public int benchmarkJoin(InMemoryJoinHash joinHash, Table probeTable)
    {
        int matches = 0;

        for (LongBlock probeBlock : probeTable.col1.blocks) {
            for (int probePosition = 0; probePosition < probeBlock.size(); probePosition++) {
                long joinPosition = joinHash.getJoinPosition(probePosition, probeBlock);
                while (joinPosition > 0) {
                    matches++;
                    joinPosition = joinHash.getNextJoinPosition(joinPosition);
                }
            }
        }

        return matches;
    }

    @Benchmark
    public int benchmarkJoinHash(JoinContext joinContext)
    {
        return benchmarkJoin(joinContext.getJoinHash(), joinContext.probeTable);
    }

    @Benchmark
    public int benchmarkJoinByteHash(JoinContext joinContext)
    {
        return benchmarkJoin(joinContext.getJoinByteHash(), joinContext.probeTable);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkJoinWithNativeArrays.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }

    private static class PagesHashStrategy
    {
        private final LongColumn hashChannel;
        private final Table buildTable;

        public PagesHashStrategy(Table buildTable)
        {
            this.buildTable = buildTable;
            this.hashChannel = buildTable.col1;
        }

        public boolean positionEqualsPosition(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition)
        {
            long leftValue = hashChannel.get(leftBlockIndex, leftPosition);
            long rightValue = hashChannel.get(rightBlockIndex, rightPosition);
            return leftValue == rightValue;
        }

        public long hashPosition(int blockIndex, int position)
        {
            return hashChannel.hash(blockIndex, position);
        }

        public long hashRow(int probePosition, LongBlock probeKey)
        {
            return probeKey.hash(probePosition);
        }

        public boolean positionEqualsRow(int leftBlockIndex, int leftPosition, int rightPosition, LongBlock rightBlock)
        {
            long leftValue = hashChannel.get(leftBlockIndex, leftPosition);
            long rightValue = rightBlock.get(rightPosition);
            return leftValue == rightValue;
        }
    }

    private static class InMemoryJoinHash
    {
        protected final LongArrayList addresses;
        protected final PagesHashStrategy pagesHashStrategy;

        protected final int mask;
        protected final int[] key;
        protected final int[] positionLinks;

        public InMemoryJoinHash(LongArrayList addresses, PagesHashStrategy pagesHashStrategy)
        {
            this.addresses = addresses;
            this.pagesHashStrategy = pagesHashStrategy;

            // reserve memory for the arrays
            int hashSize = HashCommon.arraySize(addresses.size(), 0.75f);

            mask = hashSize - 1;
            key = new int[hashSize];
            Arrays.fill(key, -1);

            this.positionLinks = new int[addresses.size()];
            Arrays.fill(positionLinks, -1);
        }

        public InMemoryJoinHash build()
        {
            // index pages
            for (int position = 0; position < addresses.size(); position++) {
                int pos = (int) getHashPosition(hashPosition(position), mask);

                // look for an empty slot or a slot containing this key
                while (key[pos] != -1) {
                    int currentKey = key[pos];
                    if (positionEqualsPosition(currentKey, position)) {
                        // found a slot for this key
                        // link the new key position to the current key position
                        positionLinks[position] = currentKey;

                        // key[pos] updated outside of this loop
                        break;
                    }
                    // increment position and mask to handler wrap around
                    pos = (pos + 1) & mask;
                }

                key[pos] = position;
            }
            return this;
        }

        public long getJoinPosition(int probePosition, LongBlock probeKeyColumn)
        {
            return getJoinPosition(probePosition, probeKeyColumn, probeKeyColumn.hash(probePosition));
        }

        public long getJoinPosition(int position, LongBlock probeKeyColumn, long rawHash)
        {
            int pos = getHashPosition(rawHash, mask);

            while (key[pos] != -1) {
                if (positionEqualsCurrentRow(key[pos], (byte) rawHash, position, probeKeyColumn)) {
                    return key[pos];
                }
                // increment position and mask to handler wrap around
                pos = (pos + 1) & mask;
            }
            return -1;
        }

        public final long getNextJoinPosition(long currentPosition)
        {
            return positionLinks[Ints.checkedCast(currentPosition)];
        }

        protected long hashPosition(int position)
        {
            long pageAddress = addresses.getLong(position);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);

            return pagesHashStrategy.hashPosition(blockIndex, blockPosition);
        }

        protected boolean positionEqualsPosition(int leftPosition, int rightPosition)
        {
            long rightPageAddress = addresses.getLong(rightPosition);
            int rightBlockIndex = decodeSliceIndex(rightPageAddress);
            int rightBlockPosition = decodePosition(rightPageAddress);

            return positionEqualsPosition(leftPosition, rightBlockIndex, rightBlockPosition);
        }

        protected boolean positionEqualsPosition(int leftPosition, int rightBlockIndex, int rightBlockPosition)
        {
            long leftPageAddress = addresses.getLong(leftPosition);
            int leftBlockIndex = decodeSliceIndex(leftPageAddress);
            int leftBlockPosition = decodePosition(leftPageAddress);

            return pagesHashStrategy.positionEqualsPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition);
        }

        protected boolean positionEqualsCurrentRow(int leftPosition, byte rawHash, int rightPosition, LongBlock rightBlock)
        {
            long pageAddress = addresses.getLong(leftPosition);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);

            return pagesHashStrategy.positionEqualsRow(blockIndex, blockPosition, rightPosition, rightBlock);
        }

        protected static int getHashPosition(long rawHash, long mask)
        {
            return (int) ((XxHash64.hash(rawHash)) & mask);
        }
    }

    private static class InMemoryJoinHashByte extends InMemoryJoinHash
    {
        private static final DataSize CACHE_SIZE = new DataSize(128, KILOBYTE);

        // Native array of hashes for faster collisions resolution compared
        // to accessing values in blocks. We use bytes to reduce memory foot print
        // and there is no performance gain from storing full hashes
        private final byte[] positionToHashes;

        public InMemoryJoinHashByte(LongArrayList addresses, PagesHashStrategy pagesHashStrategy)
        {
            super(addresses, pagesHashStrategy);

            positionToHashes = new byte[addresses.size()];
        }

        @Override
        public InMemoryJoinHash build()
        {
            // We will process addresses in batches, to save memory on array of hashes.
            int positionsInStep = Math.min(addresses.size() + 1, (int) CACHE_SIZE.toBytes() / Integer.SIZE);
            long[] positionToFullHashes = new long[positionsInStep];

            for (int step = 0; step * positionsInStep <= addresses.size(); step++) {
                int stepBeginPosition = step * positionsInStep;
                int stepEndPosition = Math.min((step + 1) * positionsInStep, addresses.size());
                int stepSize = stepEndPosition - stepBeginPosition;

                // First extract all hashes from blocks to native array.
                // Somehow having this as a separate loop is much faster compared
                // to extracting hashes on the fly in the loop below...
                for (int position = 0; position < stepSize; position++) {
                    int realPosition = position + stepBeginPosition;
                    long hash = hashPosition(realPosition);
                    positionToFullHashes[position] = hash;
                    positionToHashes[realPosition] = (byte) hash;
                }

                // index pages
                for (int position = 0; position < stepSize; position++) {
                    int realPosition = position + stepBeginPosition;
                    long hash = positionToFullHashes[position];
                    int pos = getHashPosition(hash, mask);

                    // look for an empty slot or a slot containing this key
                    while (key[pos] != -1) {
                        int currentKey = key[pos];
                        if (((byte) hash) == positionToHashes[currentKey] &&
                                positionEqualsPosition(currentKey, realPosition)) {
                            // found a slot for this key
                            // link the new key position to the current key position
                            positionLinks[realPosition] = currentKey;

                            // key[pos] updated outside of this loop
                            break;
                        }
                        // increment position and mask to handler wrap around
                        pos = (pos + 1) & mask;
                    }

                    key[pos] = realPosition;
                }
            }
            return this;
        }

        @Override
        protected boolean positionEqualsCurrentRow(int leftPosition, byte rawHash, int rightPosition, LongBlock rightBlock)
        {
            if (positionToHashes[leftPosition] != rawHash) {
                return false;
            }

            long pageAddress = addresses.getLong(leftPosition);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);

            return pagesHashStrategy.positionEqualsRow(blockIndex, blockPosition, rightPosition, rightBlock);
        }
    }
}
