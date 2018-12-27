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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ReturnPlaceConvention;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ScalarImplementationChoice;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ReturnPlaceConvention.PROVIDED_BLOCK;
import static com.facebook.presto.operator.scalar.TestProvidedBlockReturnPlaceConvention.FunctionWithProvidedBlockReturnPlaceConvention.PROVIDED_BLOCK_CONVENTION;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.util.Reflection.methodHandle;
import static org.testng.Assert.assertTrue;

public class TestProvidedBlockReturnPlaceConvention
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerScalarFunction(PROVIDED_BLOCK_CONVENTION);
    }

    @Test
    public void testProvidedBlock()
    {
        assertFunction("identity(123)", INTEGER, 123);
        assertFunction("identity(identity(123))", INTEGER, 123);
        assertFunction("identity(CAST(null AS INTEGER))", INTEGER, null);
        assertFunction("identity(identity(CAST(null AS INTEGER)))", INTEGER, null);

        assertFunction("identity(123.4E0)", DOUBLE, 123.4);
        assertFunction("identity(identity(123.4E0))", DOUBLE, 123.4);
        assertFunction("identity(CAST(null AS DOUBLE))", DOUBLE, null);
        assertFunction("identity(identity(CAST(null AS DOUBLE)))", DOUBLE, null);

        assertFunction("identity(true)", BOOLEAN, true);
        assertFunction("identity(identity(true))", BOOLEAN, true);
        assertFunction("identity(CAST(null AS BOOLEAN))", BOOLEAN, null);
        assertFunction("identity(identity(CAST(null AS BOOLEAN)))", BOOLEAN, null);

        assertFunction("identity('abc')", createVarcharType(3), "abc");
        assertFunction("identity(identity('abc'))", createVarcharType(3), "abc");
        assertFunction("identity(CAST(null AS VARCHAR))", VARCHAR, null);
        assertFunction("identity(identity(CAST(null AS VARCHAR)))", VARCHAR, null);

        assertFunction("identity(ARRAY[1,2,3])", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("identity(identity(ARRAY[1,2,3]))", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("identity(CAST(null AS ARRAY<INTEGER>))", new ArrayType(INTEGER), null);
        assertFunction("identity(identity(CAST(null AS ARRAY<INTEGER>)))", new ArrayType(INTEGER), null);

        /*
        assertFunction("identity(null)", new ArrayType(INTEGER), null);
        assertFunction("identity(ARRAY[1, 2, 3])", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertEquals(3, FunctionWithProvidedBlockReturnPlaceConvention.hitProvidedBlockBuilderBlock.get());
        assertFunction("identity(identity(ARRAY[1, 2, 3]))", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertEquals(6, FunctionWithProvidedBlockReturnPlaceConvention.hitProvidedBlockBuilderBlock.get());
        */

        assertTrue(FunctionWithProvidedBlockReturnPlaceConvention.hitProvidedBlockBuilderLong.get() > 0);
        assertTrue(FunctionWithProvidedBlockReturnPlaceConvention.hitProvidedBlockBuilderDouble.get() > 0);
        assertTrue(FunctionWithProvidedBlockReturnPlaceConvention.hitProvidedBlockBuilderBoolean.get() > 0);
        assertTrue(FunctionWithProvidedBlockReturnPlaceConvention.hitProvidedBlockBuilderSlice.get() > 0);
        assertTrue(FunctionWithProvidedBlockReturnPlaceConvention.hitProvidedBlockBuilderBlock.get() > 0);
    }

    public static class FunctionWithProvidedBlockReturnPlaceConvention
            extends SqlScalarFunction
    {
        private static final AtomicLong hitProvidedBlockBuilderLong = new AtomicLong();
        private static final AtomicLong hitProvidedBlockBuilderDouble = new AtomicLong();
        private static final AtomicLong hitProvidedBlockBuilderBoolean = new AtomicLong();
        private static final AtomicLong hitProvidedBlockBuilderSlice = new AtomicLong();
        private static final AtomicLong hitProvidedBlockBuilderBlock = new AtomicLong();

        public static final FunctionWithProvidedBlockReturnPlaceConvention PROVIDED_BLOCK_CONVENTION = new FunctionWithProvidedBlockReturnPlaceConvention();

        private static final MethodHandle METHOD_HANDLE_PROVIDED_BLOCK_LONG = methodHandle(FunctionWithProvidedBlockReturnPlaceConvention.class, "providedBlockLong", Type.class, BlockBuilder.class, long.class);
        private static final MethodHandle METHOD_HANDLE_PROVIDED_BLOCK_DOUBLE = methodHandle(FunctionWithProvidedBlockReturnPlaceConvention.class, "providedBlockDouble", Type.class, BlockBuilder.class, double.class);
        private static final MethodHandle METHOD_HANDLE_PROVIDED_BLOCK_BOOLEAN = methodHandle(FunctionWithProvidedBlockReturnPlaceConvention.class, "providedBlockBoolean", Type.class, BlockBuilder.class, boolean.class);
        private static final MethodHandle METHOD_HANDLE_PROVIDED_BLOCK_SLICE = methodHandle(FunctionWithProvidedBlockReturnPlaceConvention.class, "providedBlockSlice", Type.class, BlockBuilder.class, Slice.class);
        private static final MethodHandle METHOD_HANDLE_PROVIDED_BLOCK_BLOCK = methodHandle(FunctionWithProvidedBlockReturnPlaceConvention.class, "providedBlockBlock", Type.class, BlockBuilder.class, Block.class);

        protected FunctionWithProvidedBlockReturnPlaceConvention()
        {
            super(new Signature(
                    "identity",
                    FunctionKind.SCALAR,
                    ImmutableList.of(typeVariable("T")),
                    ImmutableList.of(),
                    parseTypeSignature("T"),
                    ImmutableList.of(parseTypeSignature("T")),
                    false));
        }

        @Override
        public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
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

            return new ScalarFunctionImplementation(
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
                                    PROVIDED_BLOCK,
                                    methodHandleProvidedBlock,
                                    Optional.empty())),
                    isDeterministic());
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
        public boolean isHidden()
        {
            return false;
        }

        @Override
        public String getDescription()
        {
            return "";
        }
    }
}
