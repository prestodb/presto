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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ReturnPlaceConvention;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ScalarImplementationChoice;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ReturnPlaceConvention.PROVIDED_BLOCKBUILDER;
import static com.facebook.presto.operator.scalar.TestProvidedBlockBuilderReturnPlaceConvention.FunctionWithProvidedBlockReturnPlaceConvention1.PROVIDED_BLOCKBUILDER_CONVENTION1;
import static com.facebook.presto.operator.scalar.TestProvidedBlockBuilderReturnPlaceConvention.FunctionWithProvidedBlockReturnPlaceConvention2.PROVIDED_BLOCKBUILDER_CONVENTION2;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.primitives.Primitives.wrap;
import static org.testng.Assert.assertTrue;

public class TestProvidedBlockBuilderReturnPlaceConvention
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerScalarFunction(PROVIDED_BLOCKBUILDER_CONVENTION1);
        registerScalarFunction(PROVIDED_BLOCKBUILDER_CONVENTION2);
    }

    @Test
    public void testProvidedBlockBuilderReturnNullOnNull()
    {
        assertFunction("identity1(123)", INTEGER, 123);
        assertFunction("identity1(identity1(123))", INTEGER, 123);
        assertFunction("identity1(CAST(null AS INTEGER))", INTEGER, null);
        assertFunction("identity1(identity1(CAST(null AS INTEGER)))", INTEGER, null);

        assertFunction("identity1(123.4E0)", DOUBLE, 123.4);
        assertFunction("identity1(identity1(123.4E0))", DOUBLE, 123.4);
        assertFunction("identity1(CAST(null AS DOUBLE))", DOUBLE, null);
        assertFunction("identity1(identity1(CAST(null AS DOUBLE)))", DOUBLE, null);

        assertFunction("identity1(true)", BOOLEAN, true);
        assertFunction("identity1(identity1(true))", BOOLEAN, true);
        assertFunction("identity1(CAST(null AS BOOLEAN))", BOOLEAN, null);
        assertFunction("identity1(identity1(CAST(null AS BOOLEAN)))", BOOLEAN, null);

        assertFunction("identity1('abc')", createVarcharType(3), "abc");
        assertFunction("identity1(identity1('abc'))", createVarcharType(3), "abc");
        assertFunction("identity1(CAST(null AS VARCHAR))", VARCHAR, null);
        assertFunction("identity1(identity1(CAST(null AS VARCHAR)))", VARCHAR, null);

        assertFunction("identity1(ARRAY[1,2,3])", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("identity1(identity1(ARRAY[1,2,3]))", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("identity1(CAST(null AS ARRAY<INTEGER>))", new ArrayType(INTEGER), null);
        assertFunction("identity1(identity1(CAST(null AS ARRAY<INTEGER>)))", new ArrayType(INTEGER), null);

        assertTrue(FunctionWithProvidedBlockReturnPlaceConvention1.hitProvidedBlockBuilderLong.get() > 0);
        assertTrue(FunctionWithProvidedBlockReturnPlaceConvention1.hitProvidedBlockBuilderDouble.get() > 0);
        assertTrue(FunctionWithProvidedBlockReturnPlaceConvention1.hitProvidedBlockBuilderBoolean.get() > 0);
        assertTrue(FunctionWithProvidedBlockReturnPlaceConvention1.hitProvidedBlockBuilderSlice.get() > 0);
        assertTrue(FunctionWithProvidedBlockReturnPlaceConvention1.hitProvidedBlockBuilderBlock.get() > 0);
    }

    @Test
    public void testProvidedBlockBuilderUseBoxedType()
    {
        assertFunction("identity2(123)", INTEGER, 123);
        assertFunction("identity2(identity2(123))", INTEGER, 123);
        assertFunction("identity2(CAST(null AS INTEGER))", INTEGER, null);
        assertFunction("identity2(identity2(CAST(null AS INTEGER)))", INTEGER, null);

        assertFunction("identity2(123.4E0)", DOUBLE, 123.4);
        assertFunction("identity2(identity2(123.4E0))", DOUBLE, 123.4);
        assertFunction("identity2(CAST(null AS DOUBLE))", DOUBLE, null);
        assertFunction("identity2(identity2(CAST(null AS DOUBLE)))", DOUBLE, null);

        assertFunction("identity2(true)", BOOLEAN, true);
        assertFunction("identity2(identity2(true))", BOOLEAN, true);
        assertFunction("identity2(CAST(null AS BOOLEAN))", BOOLEAN, null);
        assertFunction("identity2(identity2(CAST(null AS BOOLEAN)))", BOOLEAN, null);

        assertFunction("identity2('abc')", createVarcharType(3), "abc");
        assertFunction("identity2(identity2('abc'))", createVarcharType(3), "abc");
        assertFunction("identity2(CAST(null AS VARCHAR))", VARCHAR, null);
        assertFunction("identity2(identity2(CAST(null AS VARCHAR)))", VARCHAR, null);

        assertFunction("identity2(ARRAY[1,2,3])", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("identity2(identity2(ARRAY[1,2,3]))", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("identity2(CAST(null AS ARRAY<INTEGER>))", new ArrayType(INTEGER), null);
        assertFunction("identity2(identity2(CAST(null AS ARRAY<INTEGER>)))", new ArrayType(INTEGER), null);

        assertTrue(FunctionWithProvidedBlockReturnPlaceConvention2.hitProvidedBlockBuilderLong.get() > 0);
        assertTrue(FunctionWithProvidedBlockReturnPlaceConvention2.hitProvidedBlockBuilderDouble.get() > 0);
        assertTrue(FunctionWithProvidedBlockReturnPlaceConvention2.hitProvidedBlockBuilderBoolean.get() > 0);
        assertTrue(FunctionWithProvidedBlockReturnPlaceConvention2.hitProvidedBlockBuilderSlice.get() > 0);
        assertTrue(FunctionWithProvidedBlockReturnPlaceConvention2.hitProvidedBlockBuilderBlock.get() > 0);
    }

    // null convention RETURN_NULL_ON_NULL
    public static class FunctionWithProvidedBlockReturnPlaceConvention1
            extends SqlScalarFunction
    {
        private static final AtomicLong hitProvidedBlockBuilderLong = new AtomicLong();
        private static final AtomicLong hitProvidedBlockBuilderDouble = new AtomicLong();
        private static final AtomicLong hitProvidedBlockBuilderBoolean = new AtomicLong();
        private static final AtomicLong hitProvidedBlockBuilderSlice = new AtomicLong();
        private static final AtomicLong hitProvidedBlockBuilderBlock = new AtomicLong();

        public static final FunctionWithProvidedBlockReturnPlaceConvention1 PROVIDED_BLOCKBUILDER_CONVENTION1 = new FunctionWithProvidedBlockReturnPlaceConvention1();

        private static final MethodHandle METHOD_HANDLE_PROVIDED_BLOCK_LONG = methodHandle(FunctionWithProvidedBlockReturnPlaceConvention1.class, "providedBlockLong", Type.class, BlockBuilder.class, long.class);
        private static final MethodHandle METHOD_HANDLE_PROVIDED_BLOCK_DOUBLE = methodHandle(FunctionWithProvidedBlockReturnPlaceConvention1.class, "providedBlockDouble", Type.class, BlockBuilder.class, double.class);
        private static final MethodHandle METHOD_HANDLE_PROVIDED_BLOCK_BOOLEAN = methodHandle(FunctionWithProvidedBlockReturnPlaceConvention1.class, "providedBlockBoolean", Type.class, BlockBuilder.class, boolean.class);
        private static final MethodHandle METHOD_HANDLE_PROVIDED_BLOCK_SLICE = methodHandle(FunctionWithProvidedBlockReturnPlaceConvention1.class, "providedBlockSlice", Type.class, BlockBuilder.class, Slice.class);
        private static final MethodHandle METHOD_HANDLE_PROVIDED_BLOCK_BLOCK = methodHandle(FunctionWithProvidedBlockReturnPlaceConvention1.class, "providedBlockBlock", Type.class, BlockBuilder.class, Block.class);

        protected FunctionWithProvidedBlockReturnPlaceConvention1()
        {
            super(new Signature(
                    QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "identity1"),
                    FunctionKind.SCALAR,
                    ImmutableList.of(typeVariable("T")),
                    ImmutableList.of(),
                    parseTypeSignature("T"),
                    ImmutableList.of(parseTypeSignature("T")),
                    false));
        }

        @Override
        public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
        {
            Type type = boundVariables.getTypeVariable("T");
            MethodHandle methodHandleStack = MethodHandles.identity(type.getJavaType());
            MethodHandle methodHandleProvidedBlock;
            if (type.getJavaType() == long.class) {
                methodHandleProvidedBlock = METHOD_HANDLE_PROVIDED_BLOCK_LONG.bindTo(type);
            }
            else if (type.getJavaType() == double.class) {
                methodHandleProvidedBlock = METHOD_HANDLE_PROVIDED_BLOCK_DOUBLE.bindTo(type);
            }
            else if (type.getJavaType() == boolean.class) {
                methodHandleProvidedBlock = METHOD_HANDLE_PROVIDED_BLOCK_BOOLEAN.bindTo(type);
            }
            else if (type.getJavaType() == Slice.class) {
                methodHandleProvidedBlock = METHOD_HANDLE_PROVIDED_BLOCK_SLICE.bindTo(type);
            }
            else if (type.getJavaType() == Block.class) {
                methodHandleProvidedBlock = METHOD_HANDLE_PROVIDED_BLOCK_BLOCK.bindTo(type);
            }
            else {
                throw new UnsupportedOperationException();
            }

            return new BuiltInScalarFunctionImplementation(
                    ImmutableList.of(
                            new ScalarImplementationChoice(
                                    false,
                                    ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                                    ReturnPlaceConvention.STACK,
                                    methodHandleStack,
                                    Optional.empty()),
                            new ScalarImplementationChoice(
                                    false,
                                    ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                                    PROVIDED_BLOCKBUILDER,
                                    methodHandleProvidedBlock,
                                    Optional.empty())));
        }

        public static void providedBlockLong(Type type, BlockBuilder output, long value)
        {
            hitProvidedBlockBuilderLong.incrementAndGet();
            type.writeLong(output, value);
        }

        public static void providedBlockDouble(Type type, BlockBuilder output, double value)
        {
            hitProvidedBlockBuilderDouble.incrementAndGet();
            type.writeDouble(output, value);
        }

        public static void providedBlockBoolean(Type type, BlockBuilder output, boolean value)
        {
            hitProvidedBlockBuilderBoolean.incrementAndGet();
            type.writeBoolean(output, value);
        }

        public static void providedBlockSlice(Type type, BlockBuilder output, Slice value)
        {
            hitProvidedBlockBuilderSlice.incrementAndGet();
            type.writeSlice(output, value);
        }

        public static void providedBlockBlock(Type type, BlockBuilder output, Block value)
        {
            hitProvidedBlockBuilderBlock.incrementAndGet();
            type.writeObject(output, value);
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public SqlFunctionVisibility getVisibility()
        {
            return PUBLIC;
        }

        @Override
        public String getDescription()
        {
            return "";
        }
    }

    // null convention USE_BOXED_TYPE
    public static class FunctionWithProvidedBlockReturnPlaceConvention2
            extends SqlScalarFunction
    {
        private static final AtomicLong hitProvidedBlockBuilderLong = new AtomicLong();
        private static final AtomicLong hitProvidedBlockBuilderDouble = new AtomicLong();
        private static final AtomicLong hitProvidedBlockBuilderBoolean = new AtomicLong();
        private static final AtomicLong hitProvidedBlockBuilderSlice = new AtomicLong();
        private static final AtomicLong hitProvidedBlockBuilderBlock = new AtomicLong();

        public static final FunctionWithProvidedBlockReturnPlaceConvention2 PROVIDED_BLOCKBUILDER_CONVENTION2 = new FunctionWithProvidedBlockReturnPlaceConvention2();

        private static final MethodHandle METHOD_HANDLE_PROVIDED_BLOCK_LONG = methodHandle(FunctionWithProvidedBlockReturnPlaceConvention2.class, "providedBlockLong", Type.class, BlockBuilder.class, Long.class);
        private static final MethodHandle METHOD_HANDLE_PROVIDED_BLOCK_DOUBLE = methodHandle(FunctionWithProvidedBlockReturnPlaceConvention2.class, "providedBlockDouble", Type.class, BlockBuilder.class, Double.class);
        private static final MethodHandle METHOD_HANDLE_PROVIDED_BLOCK_BOOLEAN = methodHandle(FunctionWithProvidedBlockReturnPlaceConvention2.class, "providedBlockBoolean", Type.class, BlockBuilder.class, Boolean.class);
        private static final MethodHandle METHOD_HANDLE_PROVIDED_BLOCK_SLICE = methodHandle(FunctionWithProvidedBlockReturnPlaceConvention2.class, "providedBlockSlice", Type.class, BlockBuilder.class, Slice.class);
        private static final MethodHandle METHOD_HANDLE_PROVIDED_BLOCK_BLOCK = methodHandle(FunctionWithProvidedBlockReturnPlaceConvention2.class, "providedBlockBlock", Type.class, BlockBuilder.class, Block.class);

        protected FunctionWithProvidedBlockReturnPlaceConvention2()
        {
            super(new Signature(
                    QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "identity2"),
                    FunctionKind.SCALAR,
                    ImmutableList.of(typeVariable("T")),
                    ImmutableList.of(),
                    parseTypeSignature("T"),
                    ImmutableList.of(parseTypeSignature("T")),
                    false));
        }

        @Override
        public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
        {
            Type type = boundVariables.getTypeVariable("T");
            MethodHandle methodHandleStack = MethodHandles.identity(wrap(type.getJavaType()));
            MethodHandle methodHandleProvidedBlock;
            if (type.getJavaType() == long.class) {
                methodHandleProvidedBlock = METHOD_HANDLE_PROVIDED_BLOCK_LONG.bindTo(type);
            }
            else if (type.getJavaType() == double.class) {
                methodHandleProvidedBlock = METHOD_HANDLE_PROVIDED_BLOCK_DOUBLE.bindTo(type);
            }
            else if (type.getJavaType() == boolean.class) {
                methodHandleProvidedBlock = METHOD_HANDLE_PROVIDED_BLOCK_BOOLEAN.bindTo(type);
            }
            else if (type.getJavaType() == Slice.class) {
                methodHandleProvidedBlock = METHOD_HANDLE_PROVIDED_BLOCK_SLICE.bindTo(type);
            }
            else if (type.getJavaType() == Block.class) {
                methodHandleProvidedBlock = METHOD_HANDLE_PROVIDED_BLOCK_BLOCK.bindTo(type);
            }
            else {
                throw new UnsupportedOperationException();
            }

            return new BuiltInScalarFunctionImplementation(
                    ImmutableList.of(
                            new ScalarImplementationChoice(
                                    true,
                                    ImmutableList.of(valueTypeArgumentProperty(USE_BOXED_TYPE)),
                                    ReturnPlaceConvention.STACK,
                                    methodHandleStack,
                                    Optional.empty()),
                            new ScalarImplementationChoice(
                                    true,
                                    ImmutableList.of(valueTypeArgumentProperty(USE_BOXED_TYPE)),
                                    PROVIDED_BLOCKBUILDER,
                                    methodHandleProvidedBlock,
                                    Optional.empty())));
        }

        public static void providedBlockLong(Type type, BlockBuilder output, Long value)
        {
            hitProvidedBlockBuilderLong.incrementAndGet();
            if (value == null) {
                output.appendNull();
            }
            else {
                type.writeLong(output, value);
            }
        }

        public static void providedBlockDouble(Type type, BlockBuilder output, Double value)
        {
            hitProvidedBlockBuilderDouble.incrementAndGet();
            if (value == null) {
                output.appendNull();
            }
            else {
                type.writeDouble(output, value);
            }
        }

        public static void providedBlockBoolean(Type type, BlockBuilder output, Boolean value)
        {
            hitProvidedBlockBuilderBoolean.incrementAndGet();
            if (value == null) {
                output.appendNull();
            }
            else {
                type.writeBoolean(output, value);
            }
        }

        public static void providedBlockSlice(Type type, BlockBuilder output, Slice value)
        {
            hitProvidedBlockBuilderSlice.incrementAndGet();
            if (value == null) {
                output.appendNull();
            }
            else {
                type.writeSlice(output, value);
            }
        }

        public static void providedBlockBlock(Type type, BlockBuilder output, Block value)
        {
            hitProvidedBlockBuilderBlock.incrementAndGet();
            if (value == null) {
                output.appendNull();
            }
            else {
                type.writeObject(output, value);
            }
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public SqlFunctionVisibility getVisibility()
        {
            return PUBLIC;
        }

        @Override
        public String getDescription()
        {
            return "";
        }
    }
}
