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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionListBuilder;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.Signature;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.OperatorDependency;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.relational.CallExpression;
import io.prestosql.sql.relational.RowExpression;
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

import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.operator.scalar.CombineHashFunction.getHash;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.type.ArrayType.ARRAY_NULL_ELEMENT_MSG;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.relational.Expressions.field;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static io.prestosql.type.TypeUtils.checkElementNotNull;
import static io.prestosql.type.TypeUtils.hashPosition;

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
                        SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getPage()));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"$operator$hash_code", "old_hash", "another_hash"})
        private String name = FunctionRegistry.mangleOperatorName(HASH_CODE);

        @Param({"BIGINT", "VARCHAR", "DOUBLE", "BOOLEAN"})
        private String type = "BIGINT";

        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            MetadataManager metadata = MetadataManager.createTestMetadataManager();
            metadata.addFunctions(new FunctionListBuilder().scalar(BenchmarkOldArrayHash.class).getFunctions());
            metadata.addFunctions(new FunctionListBuilder().scalar(BenchmarkAnotherArrayHash.class).getFunctions());
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
            Signature signature = new Signature(name, FunctionKind.SCALAR, BIGINT.getTypeSignature(), arrayType.getTypeSignature());
            projectionsBuilder.add(new CallExpression(signature, BIGINT, ImmutableList.of(field(0, arrayType))));
            blocks[0] = createChannel(POSITIONS, ARRAY_SIZE, arrayType);

            ImmutableList<RowExpression> projections = projectionsBuilder.build();
            pageProcessor = compiler.compilePageProcessor(Optional.empty(), projections).get();
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
                @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = "T") MethodHandle hashFunction,
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
                @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = "T") MethodHandle hashFunction,
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
