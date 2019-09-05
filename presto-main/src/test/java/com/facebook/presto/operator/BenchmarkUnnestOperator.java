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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.unnest.UnnestOperator.UnnestOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.RowBlock;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 20, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkUnnestOperator
{
    private static final int TOTAL_POSITIONS = 10000;

    private static final String ARRAY_OF_VARCHAR = "array(varchar)";
    private static final String MAP_STRING_TO_STRING = "map(varchar,varchar)";
    private static final String ARRAY_OF_ROW_THREE_STRINGS = "array(row(varchar, varchar, varchar))";

    @State(Scope.Thread)
    public static class BenchmarkContext
    {
        @Param("varchar")
        private String replicateType = "varchar";

        @Param({ARRAY_OF_VARCHAR, MAP_STRING_TO_STRING, ARRAY_OF_ROW_THREE_STRINGS})
        private String nestedTypeOne = ARRAY_OF_ROW_THREE_STRINGS;

        @Param({"NONE", ARRAY_OF_VARCHAR})
        private String nestedTypeTwo = "NONE";

        @Param({"0.0", "0.2"})
        private double primitiveNullsRatioNestedOne;  // % of nulls in input primitive elements

        @Param({"0.0", "0.05"})
        private double rowNullsRatioNestedOne;    // % of nulls in row type elements

        @Param("1000")
        private int positionsPerPage = 1000;

        @Param("50")
        private int stringLengths = 50;    // max length of varchars

        @Param("300")
        private int nestedLengths = 300;   // max entries in one nested structure (array, map)

        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private OperatorFactory operatorFactory;
        private List<Page> pages;

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
            Metadata metadata = createTestMetadataManager();

            InputGenerator inputGenerator = new InputGenerator(0, nestedLengths, stringLengths);

            ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Integer> channelsBuilder = ImmutableList.builder();

            Type replicateType = getType(metadata, this.replicateType).get();
            typesBuilder.add(replicateType);
            channelsBuilder.add(0);

            Type nestedTypeOne = getType(metadata, this.nestedTypeOne).get();
            typesBuilder.add(nestedTypeOne);
            channelsBuilder.add(1);

            if (!nestedTypeTwo.equals("NONE")) {
                Type nestedTypeTwo = getType(metadata, this.nestedTypeTwo).get();
                typesBuilder.add(nestedTypeTwo);
                channelsBuilder.add(2);
            }

            List<Type> types = typesBuilder.build();
            List<Integer> channels = channelsBuilder.build();

            this.pages = createInputPages(positionsPerPage, typesBuilder.build(), inputGenerator,
                    primitiveNullsRatioNestedOne, rowNullsRatioNestedOne);

            operatorFactory = new UnnestOperatorFactory(
                    0,
                    new PlanNodeId("test"),
                    channels.subList(0, 1),
                    types.subList(0, 1),
                    channels.subList(1, channels.size()),
                    types.subList(1, types.size()),
                    true);
        }

        public Optional<Type> getType(Metadata metadata, String typeString)
        {
            if (typeString.equals("NONE")) {
                return Optional.empty();
            }
            TypeSignature signature = TypeSignature.parseTypeSignature(typeString);
            return Optional.of(metadata.getType(signature));
        }

        @TearDown
        public void cleanup()
        {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
        }

        public TaskContext createTaskContext()
        {
            return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(2, GIGABYTE));
        }

        public OperatorFactory getOperatorFactory()
        {
            return operatorFactory;
        }

        public List<Page> getPages()
        {
            return pages;
        }

        private static List<Page> createInputPages(int positionsPerPage, List<Type> types, InputGenerator inputGenerator, double primitiveNullsRatioUnnest, double rowNullsRatioUnnest)
        {
            ImmutableList.Builder<Page> pages = ImmutableList.builder();
            int pageCount = TOTAL_POSITIONS / positionsPerPage;

            for (int i = 0; i < pageCount; i++) {
                Block[] blocks = new Block[types.size()];
                blocks[0] = inputGenerator.produceBlock(types.get(0), positionsPerPage, 0.0, 0.0);
                blocks[1] = inputGenerator.produceBlock(types.get(1), positionsPerPage, primitiveNullsRatioUnnest, rowNullsRatioUnnest);
                if (blocks.length == 3) {
                    blocks[2] = inputGenerator.produceBlock(types.get(2), positionsPerPage, 0.0, 0.0);
                }
                pages.add(new Page(blocks));
            }

            return pages.build();
        }
    }

    @Benchmark
    public List<Page> unnest(BenchmarkContext context)
    {
        DriverContext driverContext = context.createTaskContext().addPipelineContext(0, true, true, false).addDriverContext();
        Operator operator = context.getOperatorFactory().createOperator(driverContext);

        Iterator<Page> input = context.getPages().iterator();
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        boolean finishing = false;
        for (int loops = 0; !operator.isFinished() && loops < 1_000_000; loops++) {
            if (operator.needsInput()) {
                if (input.hasNext()) {
                    Page inputPage = input.next();
                    operator.addInput(inputPage);
                }
                else if (!finishing) {
                    operator.finish();
                    finishing = true;
                }
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        }

        return outputPages.build();
    }

    public static class InputGenerator
    {
        private static final int INT_UPPER_LIMIT = 1_000_000_000;
        private static final String LETTERS = "abcdefghijklmnopqrstuvwxyz";

        private final Random random = new Random();

        // lower and upper bounds on nested structures in the input
        private final int minNestedCardinality;
        private final int maxNestedCardinality;

        // upper bound on length of strings generated for VARCHAR elements
        private final int maxStringLength;

        public InputGenerator(int minNestedCardinality, int maxNestedCardinality, int maxStringLength)
        {
            checkArgument(minNestedCardinality >= 0, "minNestedCardinality must be >= 0");
            checkArgument(minNestedCardinality <= maxNestedCardinality, "minNestedCardinality must be <= maxNestedCardinality");
            checkArgument(maxStringLength >= 0, "maxStringLength must be >= 0");
            this.minNestedCardinality = minNestedCardinality;
            this.maxNestedCardinality = maxNestedCardinality;
            this.maxStringLength = maxStringLength;
        }

        /**
         * Generates a block with {@code entries} positions given an input {@code type}.
         *
         * {@code primitiveNullsRatio} indicates what percentage of primitive elements
         * (VARCHAR or INT) should be null. Similarly, the value {@code rowNullsRatio}
         * indicates the percentage for Row elements.
         *
         * The null percentages are not propagated inside a MapBlock's keyBlock. Everything
         * is always non-null inside the returned keyBlock.
         *
         * The set S of valid input for {@code type} can be defined as:
         * S = {VARCHAR, INTEGER, ROW(x), ARRAY(x), MAP(x, x)} where x belongs to S
         *
         */
        public Block produceBlock(Type type, int entries, double primitiveNullsRatio, double rowNullsRatio)
        {
            if (type instanceof ArrayType) {
                return produceArrayBlock((ArrayType) type, entries, primitiveNullsRatio, rowNullsRatio);
            }
            else if (type instanceof MapType) {
                return produceMapBlock((MapType) type, entries, primitiveNullsRatio, rowNullsRatio);
            }
            else if (type instanceof RowType) {
                return produceRowBlock((RowType) type, entries, primitiveNullsRatio, rowNullsRatio);
            }
            else if (type == VARCHAR) {
                return produceStringBlock(entries, primitiveNullsRatio);
            }
            else if (type == INTEGER) {
                return produceIntBlock(entries, primitiveNullsRatio);
            }

            throw new RuntimeException("not supported");
        }

        private Block produceStringBlock(int entries, double primitiveNullsRatio)
        {
            BlockBuilder builder = VARCHAR.createBlockBuilder(null, 100);

            for (int i = 0; i < entries; i++) {
                if (random.nextDouble() <= primitiveNullsRatio) {
                    builder.appendNull();
                }
                else {
                    VARCHAR.writeString(builder, generateRandomString(random, random.nextInt(maxStringLength)));
                }
            }

            return builder.build();
        }

        private Block produceIntBlock(int entries, double primitiveNullsRatio)
        {
            BlockBuilder builder = INTEGER.createBlockBuilder(null, 100);

            for (int i = 0; i < entries; i++) {
                if (random.nextDouble() < primitiveNullsRatio) {
                    builder.appendNull();
                }
                else {
                    INTEGER.writeLong(builder, random.nextInt(INT_UPPER_LIMIT));
                }
            }

            return builder.build();
        }

        private Block produceArrayBlock(ArrayType arrayType, int entries, double primitiveNullsRatio, double rowNullsRatio)
        {
            BlockBuilder builder = arrayType.createBlockBuilder(null, 100);
            Type elementType = arrayType.getElementType();

            for (int i = 0; i < entries; i++) {
                int arrayLength = minNestedCardinality + random.nextInt(maxNestedCardinality - minNestedCardinality);
                builder.appendStructure(produceBlock(elementType, arrayLength, primitiveNullsRatio, rowNullsRatio));
            }
            return builder.build();
        }

        private Block produceMapBlock(MapType mapType, int entries, double primitiveNullsRatio, double rowNullsRatio)
        {
            int[] mapLengths = new int[entries];
            int[] offsets = new int[entries + 1];

            int elementCount = 0;
            for (int i = 0; i < entries; i++) {
                mapLengths[i] = minNestedCardinality + random.nextInt(maxNestedCardinality - minNestedCardinality);
                offsets[i + 1] = elementCount;
                elementCount += mapLengths[i];
            }

            // No null map objects OR null keys
            // This stops propagation of null percentages, but for benchmarks this is okay
            Block keyBlock = produceBlock(mapType.getKeyType(), elementCount, 0.0, 0.0);
            Block valueBlock = produceBlock(mapType.getValueType(), elementCount, primitiveNullsRatio, rowNullsRatio);

            return mapType.createBlockFromKeyValue(mapLengths.length, Optional.empty(), offsets, keyBlock, valueBlock);
        }

        public Block produceRowBlock(RowType rowType, int entries, double primitiveNullsRatio, double rowNullsRatio)
        {
            boolean[] rowIsNull = new boolean[entries];
            int nonNullCount = 0;
            for (int i = 0; i < entries; i++) {
                if (random.nextDouble() < rowNullsRatio) {
                    rowIsNull[i] = true;
                }
                else {
                    rowIsNull[i] = false;
                    nonNullCount++;
                }
            }

            int fieldCount = rowType.getTypeParameters().size();
            Block[] fieldBlocks = new Block[fieldCount];

            for (int i = 0; i < fieldCount; i++) {
                fieldBlocks[i] = produceBlock(rowType.getTypeParameters().get(i), nonNullCount, primitiveNullsRatio, rowNullsRatio);
            }

            return RowBlock.fromFieldBlocks(entries, Optional.of(rowIsNull), fieldBlocks);
        }

        public static String generateRandomString(Random random, int length)
        {
            char[] chars = new char[length];
            for (int i = 0; i < length; i++) {
                chars[i] = LETTERS.charAt(random.nextInt(LETTERS.length()));
            }
            return new String(chars);
        }
    }

    @Test
    public void testBlocks()
    {
        InputGenerator generator = new InputGenerator(0, 50, 50);

        Block block = generator.produceBlock(new ArrayType(VARCHAR), 100, 0.1, 0.1);
        assertEquals(block.getPositionCount(), 100);

        block = generator.produceBlock(mapType(VARCHAR, INTEGER), 100, 0.1, 0.1);
        assertEquals(block.getPositionCount(), 100);

        block = generator.produceBlock(RowType.anonymous(Arrays.asList(VARCHAR, VARCHAR)), 100, 0.1, 0.1);
        assertEquals(block.getPositionCount(), 100);

        block = generator.produceBlock(new ArrayType(RowType.anonymous(Arrays.asList(VARCHAR, VARCHAR, VARCHAR))), 100, 0.1, 0.1);
        assertEquals(block.getPositionCount(), 100);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkUnnestOperator.class.getSimpleName() + ".*")
                .addProfiler(GCProfiler.class)
                .build();

        new Runner(options).run();
    }
}
