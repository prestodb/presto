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

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler;
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
import java.util.Optional;
import java.util.Random;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;
import static org.testng.Assert.assertEquals;

/*
    Benchmark                                     (dataDistribution)  Mode  Cnt    Score   Error  Units
    BenchmarkOriginalJoin.benchmarkBuildByteHash              random  avgt    7   81.291 ± 1.435  ms/op
    BenchmarkOriginalJoin.benchmarkBuildByteHash       unique_random  avgt    7   40.766 ± 1.556  ms/op
    BenchmarkOriginalJoin.benchmarkBuildHash                  random  avgt    7  110.544 ± 1.346  ms/op
    BenchmarkOriginalJoin.benchmarkBuildHash           unique_random  avgt    7  115.796 ± 2.977  ms/op
    BenchmarkOriginalJoin.benchmarkJoinByteHash               random  avgt    7  163.120 ± 4.142  ms/op
    BenchmarkOriginalJoin.benchmarkJoinByteHash        unique_random  avgt    7  198.549 ± 7.777  ms/op
    BenchmarkOriginalJoin.benchmarkJoinHash                   random  avgt    7  202.685 ± 4.935  ms/op
    BenchmarkOriginalJoin.benchmarkJoinHash            unique_random  avgt    7  272.603 ± 7.136  ms/op

    no null checks
    Benchmark                                     (dataDistribution)  Mode  Cnt    Score   Error  Units
    BenchmarkOriginalJoin.benchmarkJoinByteHash               random  avgt    7  147.894 ± 5.370  ms/op
    BenchmarkOriginalJoin.benchmarkJoinByteHash        unique_random  avgt    7  183.693 ± 3.276  ms/op
    BenchmarkOriginalJoin.benchmarkJoinHash                   random  avgt    7  187.656 ± 3.160  ms/op
    BenchmarkOriginalJoin.benchmarkJoinHash            unique_random  avgt    7  245.383 ± 6.884  ms/op

    post rebase
    Benchmark                                     (dataDistribution)  Mode  Cnt    Score   Error  Units
    BenchmarkOriginalJoin.benchmarkBuildByteHash              random  avgt    7   63.195 ± 1.014  ms/op
    BenchmarkOriginalJoin.benchmarkBuildByteHash       unique_random  avgt    7   37.962 ± 0.506  ms/op
    BenchmarkOriginalJoin.benchmarkBuildHash                  random  avgt    7   92.633 ± 1.373  ms/op
    BenchmarkOriginalJoin.benchmarkBuildHash           unique_random  avgt    7   92.935 ± 2.385  ms/op
    BenchmarkOriginalJoin.benchmarkJoinByteHash               random  avgt    7  113.377 ± 1.596  ms/op
    BenchmarkOriginalJoin.benchmarkJoinByteHash        unique_random  avgt    7  135.123 ± 1.311  ms/op
    BenchmarkOriginalJoin.benchmarkJoinHash                   random  avgt    7  145.089 ± 2.292  ms/op
    BenchmarkOriginalJoin.benchmarkJoinHash            unique_random  avgt    7  175.730 ± 3.467  ms/op
 */
@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 7)
@Measurement(iterations = 7)
public class BenchmarkOriginalJoin
{
    private static final int POSITIONS_PER_BLOCK = 1024;
    private static final List<Integer> HASH_CHANNELS = ImmutableList.of(0);
    private static final JoinCompiler JOIN_COMPILER = new JoinCompiler();

    @State(Thread)
    public static class BuildContext
    {
        @Param({"random", "unique_random"})
        protected String dataDistribution = "unique_random";

        protected static final int BUILD_ROWS_NUMBER = 700_000;

        protected List<Page> buildPages;
        protected List<Type> types;

        @Setup
        public void setup()
        {
            initializeBuildTable();
        }

        protected void initializeBuildTable()
        {
            Random random = new Random();

            RowPagesBuilder buildPagesBuilder = rowPagesBuilder(BIGINT, BIGINT);

            types = buildPagesBuilder.getTypes();

            List<Integer> possibleValues = new ArrayList<>();
            for (int position = 0; position < BUILD_ROWS_NUMBER; position++) {
                possibleValues.add(position);
            }
            Collections.shuffle(possibleValues);
            Iterator<Integer> shuffledValues = possibleValues.iterator();

            int positionsInPage = 0;
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
                buildPagesBuilder.row(val1, val2);

                if (++positionsInPage >= POSITIONS_PER_BLOCK) {
                    buildPagesBuilder.pageBreak();
                    positionsInPage = 0;
                }
            }

            buildPages = buildPagesBuilder.build();
        }
    }

    @State(Thread)
    public static class JoinContext extends BuildContext
    {
        protected static final int PROBE_ROWS_NUMBER = 700_000;

        //@Param({"0.1", "1", "2"})
        protected double matchRate = 1;

        private InMemoryJoinHash joinHash;
        private InMemoryJoinHash joinByteHash;
        public List<Page> probePages;

        @Setup
        public void setup()
        {
            super.setup();
            initializeProbeTable();
        }

        protected void initializeProbeTable()
        {
            RowPagesBuilder probePagesBuilder = rowPagesBuilder(BIGINT, BIGINT);

            Random random = new Random(42);
            int remainingRows = PROBE_ROWS_NUMBER;
            List<Integer> possibleValues = new ArrayList<>();
            for (int position = 0; position < BUILD_ROWS_NUMBER; position++) {
                possibleValues.add(position);
            }
            Collections.shuffle(possibleValues);
            Iterator<Integer> shuffledValues = possibleValues.iterator();

            int positionsInPage = 0;
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
                    probePagesBuilder.row(columnA, columnB);

                    if (++positionsInPage >= POSITIONS_PER_BLOCK) {
                        probePagesBuilder.pageBreak();
                        positionsInPage = 0;
                    }

                    --remainingRows;
                }
            }

            probePages = probePagesBuilder.build();
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
        RowPagesBuilder probePagesBuilder = rowPagesBuilder(BIGINT, BIGINT);
        probePagesBuilder.row(1, 11);
        probePagesBuilder.row(2, 12);
        probePagesBuilder.row(42, 52);
        List<Page> probePages = probePagesBuilder.build();
        assertEquals(probePages.size(), 1);
        Page probePage = probePages.get(0);

        assertEquals(joinHash.getJoinPosition(0, probePage), 1);
        assertEquals(joinHash.getNextJoinPosition(1), -1);

        assertEquals(joinHash.getJoinPosition(1, probePage), -1);

        assertEquals(joinHash.getJoinPosition(2, probePage), 3);
        assertEquals(joinHash.getNextJoinPosition(3), 2);
        assertEquals(joinHash.getNextJoinPosition(2), -1);
    }

    private static List<Page> buildPages;

    static {
        RowPagesBuilder buildPagesBuilder = rowPagesBuilder(BIGINT, BIGINT);
        buildPagesBuilder.row(0, 0);
        buildPagesBuilder.row(1, 0);
        buildPagesBuilder.row(42, 0);
        buildPagesBuilder.row(42, 0);
        buildPagesBuilder.row(4, 0);

        buildPages = buildPagesBuilder.build();
    }

    @Test
    public void testOriginal()
    {
        BuildContext context = new BuildContext();
        context.setup();
        context.buildPages = buildPages;

        InMemoryJoinHash joinHash = benchmarkBuildHash(context);
        test(joinHash);
    }

    @Test
    public void testByte()
    {
        BuildContext context = new BuildContext();
        context.setup();
        context.buildPages = buildPages;

        InMemoryJoinHash joinHash = benchmarkBuildByteHash(context);
        test(joinHash);
    }

    @Benchmark
    public static InMemoryJoinHash benchmarkBuildHash(BuildContext buildContext)
    {
        LongArrayList addresses = new LongArrayList();

        int blockIndex = 0;
        for (Page page : buildContext.buildPages) {
            for (int blockPosition = 0; blockPosition < page.getPositionCount(); blockPosition++) {
                addresses.add(SyntheticAddress.encodeSyntheticAddress(blockIndex, blockPosition));
            }
            blockIndex++;
        }

        PagesHashStrategy pagesHashStrategy = JOIN_COMPILER
                .compilePagesHashStrategyFactory(buildContext.types, HASH_CHANNELS)
                .createPagesHashStrategy(extractBlocks(buildContext.buildPages), Optional.empty());

        return new InMemoryJoinHash(addresses, pagesHashStrategy).build();
    }

    private static List<? extends List<Block>> extractBlocks(List<Page> buildPages)
    {
        List<ImmutableList.Builder<Block>> columns = new ArrayList<>();
        for (Block block : buildPages.get(0).getBlocks()) {
            columns.add(ImmutableList.builder());
        }
        for (Page page : buildPages) {
            for (int column = 0; column < page.getChannelCount(); column++)  {
                columns.get(column).add(page.getBlock(column));
            }
        }

        return columns.stream().map(builder -> builder.build()).collect(toList());
    }

    @Benchmark
    public static InMemoryJoinHash benchmarkBuildByteHash(BuildContext buildContext)
    {
        LongArrayList addresses = new LongArrayList();

        int blockIndex = 0;
        for (Page page : buildContext.buildPages) {
            for (int blockPosition = 0; blockPosition < page.getPositionCount(); blockPosition++) {
                addresses.add(SyntheticAddress.encodeSyntheticAddress(blockIndex, blockPosition));
            }
            blockIndex++;
        }

        PagesHashStrategy pagesHashStrategy = JOIN_COMPILER
                .compilePagesHashStrategyFactory(buildContext.types, HASH_CHANNELS)
                .createPagesHashStrategy(extractBlocks(buildContext.buildPages), Optional.empty());

        return new InMemoryJoinHashByte(addresses, pagesHashStrategy).build();
    }

    public int benchmarkJoin(InMemoryJoinHash joinHash, List<Page> probePages)
    {
        int matches = 0;

        for (Page probePage : probePages) {
            for (int probePosition = 0; probePosition < probePage.getPositionCount(); probePosition++) {
                long joinPosition = joinHash.getJoinPosition(probePosition, probePage);
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
        return benchmarkJoin(joinContext.getJoinHash(), joinContext.probePages);
    }

    @Benchmark
    public int benchmarkJoinByteHash(JoinContext joinContext)
    {
        return benchmarkJoin(joinContext.getJoinByteHash(), joinContext.probePages);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkOriginalJoin.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
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

        public long getJoinPosition(int probePosition, Page probePage)
        {
            return getJoinPosition(probePosition, probePage, pagesHashStrategy.hashRow(probePosition, probePage));
        }

        public long getJoinPosition(int position, Page probePage, long rawHash)
        {
            int pos = getHashPosition(rawHash, mask);

            while (key[pos] != -1) {
                if (positionEqualsCurrentRow(key[pos], (byte) rawHash, position, probePage)) {
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

        protected boolean positionEqualsCurrentRow(int leftPosition, byte rawHash, int rightPosition, Page rightPage)
        {
            long pageAddress = addresses.getLong(leftPosition);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);

            return pagesHashStrategy.positionEqualsRow(blockIndex, blockPosition, rightPosition, rightPage);
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
        protected boolean positionEqualsCurrentRow(int leftPosition, byte rawHash, int rightPosition, Page rightPage)
        {
            if (positionToHashes[leftPosition] != rawHash) {
                return false;
            }

            long pageAddress = addresses.getLong(leftPosition);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);

            return pagesHashStrategy.positionEqualsRow(blockIndex, blockPosition, rightPosition, rightPage);
        }
    }
}
