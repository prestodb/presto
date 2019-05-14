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

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.client.ClientCapabilities;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.PartitionedOutputBuffer;
import com.facebook.presto.execution.buffer.TestingPartitionedOutputBuffer;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.facebook.presto.operator.PartitionedOutputOperator.PartitionedOutputFactory;
import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.LongArrayBlockBuilder;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic;
import com.facebook.presto.testing.TestingTaskContext;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.lang.invoke.MethodHandle;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZED_PARTITIONED_OUTPUT;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.spi.block.MethodHandleUtil.compose;
import static com.facebook.presto.spi.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingEnvironment.TYPE_MANAGER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.Arrays.stream;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkPartitionedOutputOperator
{
    @Benchmark
    public void addPage(BenchmarkData data)
    {
        PartitionedOutputOperator operator = data.createPartitionedOutputOperator();
        for (int i = 0; i < data.pageCount; i++) {
            operator.addInput(data.dataPage);
        }
        operator.finish();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int PARTITION_COUNT = 512;
        private static final int ROW_SIZE = 4;
        private static final int ARRAY_SIZE = 10;
        private static final int MAP_SIZE = 10;
        private static final int STRING_SIZE = 4;
        private static final int DICTIONARY_SIZE = 20;
        private static final DataSize MAX_MEMORY = new DataSize(1, GIGABYTE);
        private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-EXECUTOR-%s"));
        private static final ScheduledExecutorService SCHEDULER = newScheduledThreadPool(1, daemonThreadsNamed("test-%s"));

        //@Param({"false", "true"})
        private String useOptimized = "true";

        @Param({"1", "2"})
        private int channelCount = 2;

       // @Param({"4096", "8192"})
        private int positionCount = 8192;

        //@Param({"BIGINT", "BOOLEAN", "DECIMAL", "DOUBLE","VARCHAR","ROW(VARCHAR)", "ARRAY(BIGINT)", "ARRAY(VARCHAR)", "ARRAY(ARRAY(VARCHAR))", "MAP(BIGINT, VARCHAR)", "DICTIONARY", "RLE"})
        private String type = "ARRAY(ARRAY(VARCHAR))";

        //@Param({"true", "false"})
        private boolean hasNull = true;

        private int pageCount;
        private Page dataPage;
        private List<Type> types;

        @Setup
        public void setup()
        {
            createPages(type);
        }

        private void createPages(String inputType)
        {
            switch (inputType) {
                case "BIGINT":
                    createPagesWithBuilders(this::createBigintChannel, BIGINT);
                    pageCount = 5000;
                    break;
                case "BOOLEAN":
                    createPagesWithBuilders(this::createBooleanChannel, BOOLEAN);
                    pageCount = 5000;
                    break;
                case "DECIMAL":
                    createPagesWithBuilders(this::createLongDecimalChannel, createDecimalType());
                    pageCount = 5000;
                    break;
                case "DOUBLE":
                    createPagesWithBuilders(this::createDoubleChannel, DOUBLE);
                    pageCount = 5000;
                    break;
                case "VARCHAR":
                    createPagesWithBuilders(this::createVarcharChannel, VARCHAR);
                    pageCount = 5000;
                    break;
                case "ROW(VARCHAR)":
                    createPagesWithBuilders(this::createRowChannel, RowType.anonymous(nCopies(ROW_SIZE, VARCHAR)));
                    pageCount = 1000;
                    break;
                case "ARRAY(BIGINT)":
                    createPagesWithBuilders(this::createArrayOfBigintChannel, new ArrayType(BIGINT));
                    pageCount = 1000;
                    break;
                case "ARRAY(VARCHAR)":
                    createPagesWithBuilders(this::createArrayOfVarcharChannel, new ArrayType(VARCHAR));
                    pageCount = 1000;
                    break;
                case "ARRAY(ARRAY(VARCHAR))":
                    createPagesWithBuilders(this::createArrayOfArrayOfVarcharChannel, new ArrayType(new ArrayType(VARCHAR)));
                    pageCount = 100;
                    break;
                case "ARRAY(ARRAY(BIGINT))":
                    createPagesWithBuilders(this::createArrayOfArrayOfBigintChannel, new ArrayType(new ArrayType(BIGINT)));
                    pageCount = 1000;
                    break;
                case "MAP(BIGINT, VARCHAR)":
                    createPagesWithBuilders(this::createMapChannel, createMapType());
                    pageCount = 1000;
                    break;
                case "DICTIONARY":
                    createDictionaryPages();
                    pageCount = 3000;
                    break;
                case "RLE":
                    createRLEPages();
                    pageCount = 3000;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported dataType");
            }
        }

        private void createPagesWithBuilders(BiConsumer<BlockBuilder, Type> createChannelConsumer, Type type)
        {
            createTypes(type);

            PageBuilder pageBuilder = new PageBuilder(types);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
            createBigintChannel(blockBuilder, BIGINT);

            for (int channelIndex = 1; channelIndex <= channelCount; channelIndex++) {
                blockBuilder = pageBuilder.getBlockBuilder(channelIndex);
                createChannelConsumer.accept(blockBuilder, type);
            }
            pageBuilder.declarePositions(positionCount);

            dataPage = pageBuilder.build();
        }

        private void createDictionaryPages()
        {
            createTypes(VARBINARY);

            Block[] blocks = new Block[channelCount + 1];
            blocks[0] = createBigintChannel();
            for (int channelIndex = 1; channelIndex < channelCount + 1; channelIndex++) {
                blocks[channelIndex] = createDictionaryChannel();
            }
            dataPage = new Page(blocks);
        }

        private void createRLEPages()
        {
            createTypes(VARBINARY);

            Block[] blocks = new Block[channelCount + 1];
            blocks[0] = createBigintChannel();
            for (int channelIndex = 1; channelIndex < channelCount + 1; channelIndex++) {
                blocks[channelIndex] = createRLEChannel();
            }
            dataPage = new Page(blocks);
        }

        private void createTypes(Type type)
        {
            types = new ArrayList<>(channelCount + 1);
            types.add(0, BIGINT);
            for (int i = 1; i <= channelCount; i++) {
                types.add(i, type);
            }
        }

        private Type createMapType()
        {
            Type keyType = BIGINT;
            Type valueType = VARCHAR;
            MethodHandle keyNativeEquals = TYPE_MANAGER.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType));
            MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));
            MethodHandle keyBlockEquals = compose(keyNativeEquals, nativeValueGetter(keyType), nativeValueGetter(keyType));
            MethodHandle keyNativeHashCode = TYPE_MANAGER.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(keyType));
            MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));
            Type mapType = new MapType(
                    keyType,
                    valueType,
                    keyBlockNativeEquals,
                    keyBlockEquals,
                    keyNativeHashCode,
                    keyBlockHashCode);
            return mapType;
        }

        private Block createBigintChannel()
        {
            LongArrayBlockBuilder builder = new LongArrayBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    builder.appendNull();
                }
                else {
                    BIGINT.writeLong(builder, ThreadLocalRandom.current().nextLong());
                }
            }
            return builder.build();
        }

        private void createBigintChannel(BlockBuilder blockBuilder, Type type)
        {
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    type.writeLong(blockBuilder, position);
                }
            }
        }

        private void createBooleanChannel(BlockBuilder blockBuilder, Type type)
        {
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    type.writeBoolean(blockBuilder, ThreadLocalRandom.current().nextBoolean());
                }
            }
        }

        private void createDoubleChannel(BlockBuilder blockBuilder, Type type)
        {
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    type.writeDouble(blockBuilder, position);
                }
            }
        }

        private void createLongDecimalChannel(BlockBuilder blockBuilder, Type decimalType)
        {
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    Slice decimal = UnscaledDecimal128Arithmetic.unscaledDecimal();
                    decimal.setDouble(0, ThreadLocalRandom.current().nextDouble());

                    decimalType.writeSlice(blockBuilder, decimal);
                }
            }
        }

        private void createVarcharChannel(BlockBuilder blockBuilder, Type type)
        {
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    type.writeSlice(blockBuilder, Slices.utf8Slice(getRandomString(ThreadLocalRandom.current().nextInt(STRING_SIZE))));
                }
            }
        }

        private void createRowChannel(BlockBuilder blockBuilder, Type type)
        {
            List<Type> subTypes = ((RowType) type).getTypeParameters();
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    BlockBuilder singleRowBlockWriter = blockBuilder.beginBlockEntry();
                    for (Type subType : subTypes) {
                        subType.writeSlice(singleRowBlockWriter, utf8Slice(getRandomString(STRING_SIZE)));
                    }
                    blockBuilder.closeEntry();
                }
            }
        }

        private void createArrayOfBigintChannel(BlockBuilder blockBuilder, Type type)
        {
            ArrayType arrayType = (ArrayType) type;
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
                    for (int i = 0; i < ARRAY_SIZE; i++) {
                        arrayType.getElementType().writeLong(entryBuilder, ThreadLocalRandom.current().nextLong());
                    }
                    blockBuilder.closeEntry();
                }
            }
        }

        private void createArrayOfVarcharChannel(BlockBuilder blockBuilder, Type type)
        {
            ArrayType arrayType = (ArrayType) type;
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
                    for (int i = 0; i < ARRAY_SIZE; i++) {
                        if (hasNull && i % 10 == 0) {
                            entryBuilder.appendNull();
                        }
                        else {
                            arrayType.getElementType().writeSlice(entryBuilder, Slices.wrappedBuffer(getRandomString(STRING_SIZE).getBytes()));
                        }
                    }
                    blockBuilder.closeEntry();
                }
            }
        }

        private void createArrayOfArrayOfVarcharChannel(BlockBuilder blockBuilder, Type type)
        {
            ArrayType arrayType = (ArrayType) type;
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    BlockBuilder entryBuilder1 = blockBuilder.beginBlockEntry();
                    for (int i = 0; i < ARRAY_SIZE; i++) {
                        if (hasNull && i % 10 == 0) {
                            entryBuilder1.appendNull();
                        }
                        else {
                            BlockBuilder entryBuilder2 = entryBuilder1.beginBlockEntry();
                            for (int j = 0; j < ARRAY_SIZE; j++) {
                                if (hasNull && j % 10 == 0) {
                                    entryBuilder2.appendNull();
                                }
                                else {
                                    ((ArrayType) arrayType.getElementType()).getElementType().writeSlice(entryBuilder2, Slices.wrappedBuffer(getRandomString(8).getBytes()));
                                }
                            }
                            entryBuilder1.closeEntry();
                        }
                    }
                    blockBuilder.closeEntry();
                }
            }
        }

        private void createArrayOfArrayOfBigintChannel(BlockBuilder blockBuilder, Type type)
        {
            ArrayType arrayType = (ArrayType) type;
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    BlockBuilder entryBuilder1 = blockBuilder.beginBlockEntry();
                    for (int i = 0; i < ARRAY_SIZE; i++) {
                        if (hasNull && i % 10 == 0) {
                            entryBuilder1.appendNull();
                        }
                        else {
                            BlockBuilder entryBuilder2 = entryBuilder1.beginBlockEntry();
                            for (int j = 0; j < ARRAY_SIZE; j++) {
                                if (hasNull && j % 10 == 0) {
                                    entryBuilder2.appendNull();
                                }
                                else {
                                    ((ArrayType) arrayType.getElementType()).getElementType().writeLong(entryBuilder2, ThreadLocalRandom.current().nextLong());
                                }
                            }
                            entryBuilder1.closeEntry();
                        }
                    }
                    blockBuilder.closeEntry();
                }
            }
        }

        private void createMapChannel(BlockBuilder blockBuilder, Type type)
        {
            MapType mapType = (MapType) type;
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
                    for (int i = 0; i < MAP_SIZE; i++) {
                        mapType.getKeyType().writeLong(entryBuilder, position + i);
                        mapType.getValueType().writeSlice(entryBuilder, Slices.utf8Slice(getRandomString(ThreadLocalRandom.current().nextInt(STRING_SIZE))));
                    }
                    blockBuilder.closeEntry();
                }
            }
        }

        private Block createDictionaryChannel()
        {
            BlockBuilder dictionaryBuilder = VARBINARY.createBlockBuilder(null, 100);
            for (int position = 0; position < DICTIONARY_SIZE; position++) {
                VARBINARY.writeSlice(dictionaryBuilder, utf8Slice("testString" + position));
            }
            Block dictionary = dictionaryBuilder.build();

            int[] ids = new int[positionCount];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = ThreadLocalRandom.current().nextInt(DICTIONARY_SIZE);
            }
            return new DictionaryBlock(dictionary, ids);
        }

        private Block createRLEChannel()
        {
            return RunLengthEncodedBlock.create(VARCHAR, utf8Slice(getRandomString(100)), positionCount);
        }

        private static String getRandomString(int size)
        {
            byte[] array = new byte[size];
            ThreadLocalRandom.current().nextBytes(array);
            return new String(array, Charset.forName("UTF-8"));
        }

        private PartitionedOutputOperator createPartitionedOutputOperator()
        {
            PartitionFunction partitionFunction = new LocalPartitionGenerator(new PrecomputedHashGenerator(0), PARTITION_COUNT);

            //PartitionFunction partitionFunction = new LocalPartitionGenerator(new InterpretedHashGenerator(ImmutableList.of(BIGINT), new int[] {0}), PARTITION_COUNT);
            PagesSerdeFactory serdeFactory = new PagesSerdeFactory(new BlockEncodingManager(new TypeRegistry()), false);
            OutputBuffers buffers = createInitialEmptyOutputBuffers(PARTITIONED);
            for (int partition = 0; partition < PARTITION_COUNT; partition++) {
                buffers = buffers.withBuffer(new OutputBuffers.OutputBufferId(partition), partition);
            }
            PartitionedOutputBuffer buffer = createPartitionedBuffer(
                    buffers.withNoMoreBufferIds(),
                    new DataSize(Long.MAX_VALUE, BYTE)); // don't let output buffer block
            buffer.registerLifespanCompletionCallback(ignore -> {});
            PartitionedOutputFactory operatorFactory = new PartitionedOutputFactory(
                    partitionFunction,
                    ImmutableList.of(0),
                    ImmutableList.of(Optional.empty()),
                    false,
                    OptionalInt.empty(),
                    buffer,
                    new DataSize(1, GIGABYTE));
            return (PartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), types, Function.identity(), serdeFactory)
                    .createOperator(createDriverContext());
        }

        private DriverContext createDriverContext()
        {
            Session testSession = testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema(TINY_SCHEMA_NAME)
                    .setClientCapabilities(stream(ClientCapabilities.values())
                            .map(ClientCapabilities::toString)
                            .collect(toImmutableSet()))
                    .setSystemProperty(OPTIMIZED_PARTITIONED_OUTPUT, useOptimized)
                    .build();

            return TestingTaskContext.builder(EXECUTOR, SCHEDULER, testSession)
                    .setMemoryPoolSize(MAX_MEMORY)
                    .build()
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();
        }

        private TestingPartitionedOutputBuffer createPartitionedBuffer(OutputBuffers buffers, DataSize dataSize)
        {
            return new TestingPartitionedOutputBuffer(
                    "task-instance-id",
                    new StateMachine<>("bufferState", SCHEDULER, OPEN, TERMINAL_BUFFER_STATES),
                    buffers,
                    dataSize,
                    () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                    SCHEDULER);
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkPartitionedOutputOperator().addPage(data);
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .jvmArgs("-Xmx80g")
                .include(".*" + BenchmarkPartitionedOutputOperator.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
