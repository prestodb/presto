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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
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

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.BenchmarkArrayFilter.ExactArrayFilterFunction.EXACT_ARRAY_FILTER_FUNCTION;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.Boolean.TRUE;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrayFilter
{
    private static final int POSITIONS = 100_000;
    private static final int ARRAY_SIZE = 4;
    private static final int NUM_TYPES = 1;
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);

    static {
        Verify.verify(NUM_TYPES == TYPES.size());
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS * ARRAY_SIZE * NUM_TYPES)
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
        @Param({"filter", "exact_filter"})
        private String name = "filter";

        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            MetadataManager metadata = MetadataManager.createTestMetadataManager();
            FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
            metadata.registerBuiltInFunctions(new FunctionListBuilder().function(EXACT_ARRAY_FILTER_FUNCTION).getFunctions());
            ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
            ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();
            Block[] blocks = new Block[TYPES.size()];
            for (int i = 0; i < TYPES.size(); i++) {
                Type elementType = TYPES.get(i);
                ArrayType arrayType = new ArrayType(elementType);
                FunctionHandle functionHandle = functionAndTypeManager.lookupFunction(name, fromTypeSignatures(arrayType.getTypeSignature(), parseTypeSignature("function(bigint,boolean)")));
                FunctionHandle greaterThan = functionAndTypeManager.resolveOperator(GREATER_THAN, fromTypes(BIGINT, BIGINT));
                projectionsBuilder.add(new CallExpression(name, functionHandle, arrayType, ImmutableList.of(
                        field(0, arrayType),
                        new LambdaDefinitionExpression(
                                ImmutableList.of(BIGINT),
                                ImmutableList.of("x"),
                                new CallExpression(GREATER_THAN.name(), greaterThan, BOOLEAN, ImmutableList.of(new VariableReferenceExpression("x", BIGINT), constant(0L, BIGINT)))))));
                blocks[i] = createChannel(POSITIONS, ARRAY_SIZE, arrayType);
            }

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
        new BenchmarkArrayFilter().benchmark(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkArrayFilter.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }

    public static final class ExactArrayFilterFunction
            extends SqlScalarFunction
    {
        public static final ExactArrayFilterFunction EXACT_ARRAY_FILTER_FUNCTION = new ExactArrayFilterFunction();

        private static final MethodHandle METHOD_HANDLE = methodHandle(ExactArrayFilterFunction.class, "filter", Type.class, Block.class, MethodHandle.class);

        private ExactArrayFilterFunction()
        {
            super(new Signature(
                    QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "exact_filter"),
                    FunctionKind.SCALAR,
                    ImmutableList.of(typeVariable("T")),
                    ImmutableList.of(),
                    parseTypeSignature("array(T)"),
                    ImmutableList.of(parseTypeSignature("array(T)"), parseTypeSignature("function(T,boolean)")),
                    false));
        }

        @Override
        public SqlFunctionVisibility getVisibility()
        {
            return PUBLIC;
        }

        @Override
        public boolean isDeterministic()
        {
            return false;
        }

        @Override
        public String getDescription()
        {
            return "return array containing elements that match the given predicate";
        }

        @Override
        public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
        {
            Type type = boundVariables.getTypeVariable("T");
            return new BuiltInScalarFunctionImplementation(
                    false,
                    ImmutableList.of(
                            valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                            valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                    METHOD_HANDLE.bindTo(type));
        }

        public static Block filter(Type type, Block block, MethodHandle function)
        {
            int positionCount = block.getPositionCount();
            BlockBuilder resultBuilder = type.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                Long input = (Long) readNativeValue(type, block, position);
                Boolean keep;
                try {
                    keep = (Boolean) function.invokeExact(input);
                }
                catch (Throwable t) {
                    throwIfUnchecked(t);
                    throw new RuntimeException(t);
                }
                if (TRUE.equals(keep)) {
                    block.writePositionTo(position, resultBuilder);
                }
            }
            return resultBuilder.build();
        }
    }
}
