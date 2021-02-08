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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
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
import org.openjdk.jmh.runner.options.WarmupMode;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.type.ArrayType.ARRAY_NULL_ELEMENT_MSG;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.operator.scalar.CombineHashFunction.getHash;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.type.TypeUtils.checkElementNotNull;
import static com.facebook.presto.type.TypeUtils.hashPosition;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrayHashCodeOperator
{
    private static final int POSITIONS = 100_000;
    private static final int ARRAY_SIZE = 100;
    private static final int NUM_TYPES = 4;

    @Benchmark
    @OperationsPerInvocation(POSITIONS * ARRAY_SIZE * NUM_TYPES)
    public List<Optional<Page>> arrayHashCode(BenchmarkData data)
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
        @Param({"$operator$hash_code", "old_hash", "another_hash"})
        private String name = HASH_CODE.getFunctionName().getObjectName();

        @Param({"BIGINT", "VARCHAR", "DOUBLE", "BOOLEAN"})
        private String type = "BIGINT";

        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            MetadataManager metadata = MetadataManager.createTestMetadataManager();
            FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
            metadata.registerBuiltInFunctions(new FunctionListBuilder().scalar(BenchmarkOldArrayHash.class).getFunctions());
            metadata.registerBuiltInFunctions(new FunctionListBuilder().scalar(BenchmarkAnotherArrayHash.class).getFunctions());
            ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
            ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();
            Block[] blocks = new Block[1];
            Type elementType;
            switch (type) {
                case "BIGINT":
                    elementType = BIGINT;
                    break;
                case "VARCHAR":
                    elementType = VARCHAR;
                    break;
                case "DOUBLE":
                    elementType = DOUBLE;
                    break;
                case "BOOLEAN":
                    elementType = BOOLEAN;
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            ArrayType arrayType = new ArrayType(elementType);
            FunctionHandle functionHandle = functionAndTypeManager.lookupFunction(name, fromTypes(arrayType));
            projectionsBuilder.add(new CallExpression(name, functionHandle, BIGINT, ImmutableList.of(field(0, arrayType))));
            blocks[0] = createChannel(POSITIONS, ARRAY_SIZE, arrayType);

            ImmutableList<RowExpression> projections = projectionsBuilder.build();
            pageProcessor = compiler.compilePageProcessor(SESSION.getSqlFunctionProperties(), Optional.empty(), projections).get();
            page = new Page(blocks);
        }

        private static Block createChannel(int positionCount, int arraySize, ArrayType arrayType)
        {
            BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
                for (int i = 0; i < arraySize; i++) {
                    if (arrayType.getElementType().getJavaType() == long.class) {
                        arrayType.getElementType().writeLong(entryBuilder, ThreadLocalRandom.current().nextLong());
                    }
                    else if (arrayType.getElementType().getJavaType() == double.class) {
                        arrayType.getElementType().writeDouble(entryBuilder, ThreadLocalRandom.current().nextDouble());
                    }
                    else if (arrayType.getElementType().getJavaType() == boolean.class) {
                        arrayType.getElementType().writeBoolean(entryBuilder, ThreadLocalRandom.current().nextBoolean());
                    }
                    else if (arrayType.getElementType().equals(VARCHAR)) {
                        arrayType.getElementType().writeSlice(entryBuilder, Slices.utf8Slice("test_string"));
                    }
                    else {
                        throw new UnsupportedOperationException();
                    }
                }
                blockBuilder.closeEntry();
            }
            return blockBuilder.build();
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

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkArrayHashCodeOperator().arrayHashCode(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .warmupMode(WarmupMode.BULK)
                .include(".*" + BenchmarkArrayHashCodeOperator.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }

    @ScalarFunction("old_hash")
    @Description("Only for benchmark array hash code operator")
    public static final class BenchmarkOldArrayHash
    {
        private BenchmarkOldArrayHash() {}

        @TypeParameter("T")
        @SqlType(StandardTypes.BIGINT)
        public static long oldHash(
                @OperatorDependency(operator = HASH_CODE, argumentTypes = "T") MethodHandle hashFunction,
                @TypeParameter("T") Type type,
                @SqlType("array(T)") Block block)
        {
            int hash = 0;
            for (int i = 0; i < block.getPositionCount(); i++) {
                checkElementNotNull(block.isNull(i), ARRAY_NULL_ELEMENT_MSG);
                hash = (int) getHash(hash, type.hash(block, i));
            }
            return hash;
        }
    }

    @ScalarFunction("another_hash")
    @Description("Only for benchmark array hash code operator")
    public static final class BenchmarkAnotherArrayHash
    {
        private BenchmarkAnotherArrayHash() {}

        @TypeParameter("T")
        @SqlType(StandardTypes.BIGINT)
        public static long anotherHash(
                @OperatorDependency(operator = HASH_CODE, argumentTypes = "T") MethodHandle hashFunction,
                @TypeParameter("T") Type type,
                @SqlType("array(T)") Block block)
        {
            int hash = 0;
            for (int i = 0; i < block.getPositionCount(); i++) {
                checkElementNotNull(block.isNull(i), ARRAY_NULL_ELEMENT_MSG);
                hash = (int) getHash(hash, hashPosition(hashFunction, type, block, i));
            }
            return hash;
        }
    }
}
