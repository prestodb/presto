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

import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.operator.scalar.CombineHashFunction.getHash;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.type.ArrayType.ARRAY_NULL_ELEMENT_MSG;
import static com.facebook.presto.type.TypeUtils.checkElementNotNull;
import static com.facebook.presto.type.TypeUtils.hashPosition;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrayHashCodeOperator
{
    private static final int POSITIONS = 100_000;
    private static final int ARRAY_SIZE = 100;
    private static final int NUM_TYPES = 4;

    @Benchmark
    @OperationsPerInvocation(POSITIONS * ARRAY_SIZE * NUM_TYPES)
    public Object arrayHashCode(BenchmarkData data)
            throws Throwable
    {
        int position = 0;
        List<Page> pages = new ArrayList<>();
        while (position < data.getPage().getPositionCount()) {
            position = data.getPageProcessor().process(SESSION, data.getPage(), position, data.getPage().getPositionCount(), data.getPageBuilder());
            pages.add(data.getPageBuilder().build());
            data.getPageBuilder().reset();
        }
        return pages;
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"$operator$hash_code", "old_hash", "another_hash"})
        private String name = FunctionRegistry.mangleOperatorName(HASH_CODE);

        @Param({"BIGINT", "VARCHAR", "DOUBLE", "BOOLEAN"})
        private String type = "BIGINT";

        private PageBuilder pageBuilder;
        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            MetadataManager metadata = MetadataManager.createTestMetadataManager();
            metadata.addFunctions(new FunctionListBuilder().scalar(BenchmarkOldArrayHash.class).getFunctions());
            metadata.addFunctions(new FunctionListBuilder().scalar(BenchmarkAnotherArrayHash.class).getFunctions());
            ExpressionCompiler compiler = new ExpressionCompiler(metadata);
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
            projectionsBuilder.add(new CallExpression(signature, BIGINT, ImmutableList.of(new InputReferenceExpression(0, arrayType))));
            blocks[0] = createChannel(POSITIONS, ARRAY_SIZE, arrayType);

            ImmutableList<RowExpression> projections = projectionsBuilder.build();
            pageProcessor = compiler.compilePageProcessor(new ConstantExpression(true, BOOLEAN), projections).get();
            pageBuilder = new PageBuilder(projections.stream().map(RowExpression::getType).collect(Collectors.toList()));
            page = new Page(blocks);
        }

        private static Block createChannel(int positionCount, int arraySize, ArrayType arrayType)
        {
            BlockBuilder blockBuilder = arrayType.createBlockBuilder(new BlockBuilderStatus(), positionCount);
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

        public PageBuilder getPageBuilder()
        {
            return pageBuilder;
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
                @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle hashFunction,
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
                @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle hashFunction,
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
