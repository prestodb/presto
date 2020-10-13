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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.runner.options.WarmupMode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(10)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkJsonToArrayCast
{
    private static final int POSITION_COUNT = 100_000;
    private static final int ARRAY_SIZE = 20;

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public List<Optional<Page>> benchmark(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getPageProcessor().process(
                        SESSION.getSqlFunctionProperties(),
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getPage()));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"BIGINT", "DOUBLE", "VARCHAR"})
        private String valueTypeName = "BIGINT";

        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            Type elementType;
            switch (valueTypeName) {
                case "BIGINT":
                    elementType = BIGINT;
                    break;
                case "DOUBLE":
                    elementType = DOUBLE;
                    break;
                case "VARCHAR":
                    elementType = VARCHAR;
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

            MetadataManager metadata = createTestMetadataManager();
            FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
            FunctionHandle functionHandle = functionAndTypeManager.lookupCast(CAST, JSON.getTypeSignature(), (new ArrayType(elementType)).getTypeSignature());

            List<RowExpression> projections = ImmutableList.of(
                    new CallExpression(CAST.name(), functionHandle, new ArrayType(elementType), ImmutableList.of(field(0, JSON))));

            pageProcessor = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0))
                    .compilePageProcessor(SESSION.getSqlFunctionProperties(), Optional.empty(), projections)
                    .get();

            page = new Page(createChannel(POSITION_COUNT, ARRAY_SIZE, elementType));
        }

        private static Block createChannel(int positionCount, int mapSize, Type elementType)
        {
            BlockBuilder blockBuilder = JSON.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                SliceOutput jsonSlice = new DynamicSliceOutput(20 * mapSize);
                jsonSlice.appendByte('[');
                for (int i = 0; i < mapSize; i++) {
                    if (i != 0) {
                        jsonSlice.appendByte(',');
                    }
                    String value = generateRandomJsonValue(elementType);
                    jsonSlice.appendBytes(value.getBytes());
                }
                jsonSlice.appendByte(']');

                JSON.writeSlice(blockBuilder, jsonSlice.slice());
            }
            return blockBuilder.build();
        }

        private static String generateRandomJsonValue(Type valueType)
        {
            if (valueType == BIGINT) {
                return Long.toString(ThreadLocalRandom.current().nextLong());
            }
            else if (valueType == DOUBLE) {
                return Double.toString(ThreadLocalRandom.current().nextDouble());
            }
            else if (valueType == VARCHAR) {
                String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

                int length = ThreadLocalRandom.current().nextInt(10) + 1;
                StringBuilder builder = new StringBuilder(length + 2);
                builder.append('"');
                for (int i = 0; i < length; i++) {
                    builder.append(characters.charAt(ThreadLocalRandom.current().nextInt(characters.length())));
                }
                builder.append('"');
                return builder.toString();
            }
            else {
                throw new UnsupportedOperationException();
            }
        }

        public PageProcessor getPageProcessor()
        {
            return pageProcessor;
        }

        public Page getPage()
        {
            return page;
        }
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkJsonToArrayCast().benchmark(data);
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkJsonToArrayCast().benchmark(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkJsonToArrayCast.class.getSimpleName() + ".*")
                .warmupMode(WarmupMode.BULK_INDI)
                .build();
        new Runner(options).run();
    }
}
