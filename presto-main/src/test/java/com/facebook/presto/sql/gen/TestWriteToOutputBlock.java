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
package com.facebook.presto.sql.gen;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.gen.TestWriteToOutputBlock.TestIdentity.IDENTITY;
import static com.facebook.presto.sql.gen.TestWriteToOutputBlock.TestNullSwitch.NULL_SWITCH;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;

public class TestWriteToOutputBlock
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerScalarFunction(IDENTITY);
        registerScalarFunction(NULL_SWITCH);
    }

    @Test
    public void test()
            throws Exception
    {
        assertFunction("identity(123)", INTEGER, 123);
        assertFunction("identity(identity(123))", INTEGER, 123);
        assertFunction("identity(CAST(null AS INTEGER))", INTEGER, null);
        assertFunction("identity(identity(CAST(null AS INTEGER)))", INTEGER, null);

        assertFunction("identity(123.4)", DOUBLE, 123.4);
        assertFunction("identity(identity(123.4))", DOUBLE, 123.4);
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

        assertFunction("null_switch(123)", INTEGER, null);
        assertFunction("null_switch(null_switch(123))", INTEGER, 123);
        assertFunction("null_switch(CAST(null AS INTEGER))", INTEGER, 123);
        assertFunction("null_switch(null_switch(CAST(null AS INTEGER)))", INTEGER, null);

        assertFunction("null_switch(123.4)", DOUBLE, null);
        assertFunction("null_switch(null_switch(123.4))", DOUBLE, 123.4);
        assertFunction("null_switch(CAST(null AS DOUBLE))", DOUBLE, 123.4);
        assertFunction("null_switch(null_switch(CAST(null AS DOUBLE)))", DOUBLE, null);

        assertFunction("null_switch(true)", BOOLEAN, null);
        assertFunction("null_switch(null_switch(true))", BOOLEAN, true);
        assertFunction("null_switch(CAST(null AS BOOLEAN))", BOOLEAN, true);
        assertFunction("null_switch(null_switch(CAST(null AS BOOLEAN)))", BOOLEAN, null);

        assertFunction("null_switch('abc')", VarcharType.createVarcharType(3), null);
        assertFunction("null_switch(null_switch('abc'))", createVarcharType(3), "abc");
        assertFunction("null_switch(CAST(null AS VARCHAR))", VARCHAR, "abc");
        assertFunction("null_switch(null_switch(CAST(null AS VARCHAR)))", VARCHAR, null);

        assertFunction("null_switch(ARRAY[1,2,3])", new ArrayType(INTEGER), null);
        assertFunction("null_switch(null_switch(ARRAY[1,2,3]))", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("null_switch(CAST(null AS ARRAY<INTEGER>))", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("null_switch(null_switch(CAST(null AS ARRAY<INTEGER>)))", new ArrayType(INTEGER), null);
    }

    public static class TestIdentity
            extends SqlScalarFunction
    {
        public static final TestIdentity IDENTITY = new TestIdentity();
        private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(TestIdentity.class, "identityLong", Type.class, BlockBuilder.class, long.class);
        private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(TestIdentity.class, "identityDouble", Type.class, BlockBuilder.class, double.class);
        private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(TestIdentity.class, "identityBoolean", Type.class, BlockBuilder.class, boolean.class);
        private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(TestIdentity.class, "identitySlice", Type.class, BlockBuilder.class, Slice.class);
        private static final MethodHandle METHOD_HANDLE_BLOCK = methodHandle(TestIdentity.class, "identityBlock", Type.class, BlockBuilder.class, Block.class);

        private TestIdentity()
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
        public boolean isHidden()
        {
            return false;
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public String getDescription()
        {
            return "return the same value";
        }

        @Override
        public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            Type type = boundVariables.getTypeVariable("T");
            MethodHandle methodHandle = null;
            if (type.getJavaType() == long.class) {
                methodHandle = METHOD_HANDLE_LONG.bindTo(type);
            }
            else if (type.getJavaType() == double.class) {
                methodHandle = METHOD_HANDLE_DOUBLE.bindTo(type);
            }
            else if (type.getJavaType() == boolean.class) {
                methodHandle = METHOD_HANDLE_BOOLEAN.bindTo(type);
            }
            else if (type.getJavaType() == Slice.class) {
                methodHandle = METHOD_HANDLE_SLICE.bindTo(type);
            }
            else if (type.getJavaType() == Block.class) {
                methodHandle = METHOD_HANDLE_BLOCK.bindTo(type);
            }
            else {
                checkState(false);
            }

            return new ScalarFunctionImplementation(
                    false,
                    ImmutableList.of(false),
                    ImmutableList.of(false),
                    ImmutableList.of(Optional.empty()),
                    methodHandle,
                    Optional.empty(),
                    true,
                    isDeterministic());
        }

        public static void identityLong(Type type, BlockBuilder outputBlock, long value)
        {
            type.writeLong(outputBlock, value);
        }

        public static void identityDouble(Type type, BlockBuilder outputBlock, double value)
        {
            type.writeDouble(outputBlock, value);
        }

        public static void identityBoolean(Type type, BlockBuilder outputBlock, boolean value)
        {
            type.writeBoolean(outputBlock, value);
        }

        public static void identitySlice(Type type, BlockBuilder outputBlock, Slice value)
        {
            type.writeSlice(outputBlock, value);
        }

        public static void identityBlock(Type type, BlockBuilder outputBlock, Block value)
        {
            type.writeObject(outputBlock, value);
        }
    }

    public static class TestNullSwitch
            extends SqlScalarFunction
    {
        public static final TestNullSwitch NULL_SWITCH = new TestNullSwitch();
        private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(TestNullSwitch.class, "nullSwitchLong", Type.class, BlockBuilder.class, long.class, boolean.class);
        private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(TestNullSwitch.class, "nullSwitchDouble", Type.class, BlockBuilder.class, double.class, boolean.class);
        private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(TestNullSwitch.class, "nullSwitchBoolean", Type.class, BlockBuilder.class, boolean.class, boolean.class);
        private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(TestNullSwitch.class, "nullSwitchSlice", Type.class, BlockBuilder.class, Slice.class, boolean.class);
        private static final MethodHandle METHOD_HANDLE_BLOCK = methodHandle(TestNullSwitch.class, "nullSwitchBlock", Type.class, BlockBuilder.class, Block.class, boolean.class);

        private TestNullSwitch()
        {
            super(new Signature(
                    "null_switch",
                    FunctionKind.SCALAR,
                    ImmutableList.of(typeVariable("T")),
                    ImmutableList.of(),
                    parseTypeSignature("T"),
                    ImmutableList.of(parseTypeSignature("T")),
                    false));
        }

        @Override
        public boolean isHidden()
        {
            return false;
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public String getDescription()
        {
            return "switch between null and predefined value";
        }

        @Override
        public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            Type type = boundVariables.getTypeVariable("T");
            MethodHandle methodHandle = null;
            if (type.getJavaType() == long.class) {
                methodHandle = METHOD_HANDLE_LONG.bindTo(type);
            }
            else if (type.getJavaType() == double.class) {
                methodHandle = METHOD_HANDLE_DOUBLE.bindTo(type);
            }
            else if (type.getJavaType() == boolean.class) {
                methodHandle = METHOD_HANDLE_BOOLEAN.bindTo(type);
            }
            else if (type.getJavaType() == Slice.class) {
                methodHandle = METHOD_HANDLE_SLICE.bindTo(type);
            }
            else if (type.getJavaType() == Block.class) {
                methodHandle = METHOD_HANDLE_BLOCK.bindTo(type);
            }
            else {
                checkState(false);
            }

            return new ScalarFunctionImplementation(
                    true,
                    ImmutableList.of(true),
                    ImmutableList.of(true),
                    ImmutableList.of(Optional.empty()),
                    methodHandle,
                    Optional.empty(),
                    true,
                    isDeterministic());
        }

        public static void nullSwitchLong(Type type, BlockBuilder outputBlock, long value, boolean isNull)
        {
            if (isNull) {
                type.writeLong(outputBlock, 123);
            }
            else {
                outputBlock.appendNull();
            }
        }

        public static void nullSwitchDouble(Type type, BlockBuilder outputBlock, double value, boolean isNull)
        {
            if (isNull) {
                type.writeDouble(outputBlock, 123.4);
            }
            else {
                outputBlock.appendNull();
            }
        }

        public static void nullSwitchBoolean(Type type, BlockBuilder outputBlock, boolean value, boolean isNull)
        {
            if (isNull) {
                type.writeBoolean(outputBlock, true);
            }
            else {
                outputBlock.appendNull();
            }
        }

        public static void nullSwitchSlice(Type type, BlockBuilder outputBlock, Slice value, boolean isNull)
        {
            if (isNull) {
                type.writeSlice(outputBlock, utf8Slice("abc"));
            }
            else {
                outputBlock.appendNull();
            }
        }

        public static void nullSwitchBlock(Type type, BlockBuilder outputBlock, Block value, boolean isNull)
        {
            checkArgument(type.equals(new ArrayType(INTEGER)));

            if (isNull) {
                BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 1);
                BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
                INTEGER.writeLong(entryBuilder, 1);
                INTEGER.writeLong(entryBuilder, 2);
                INTEGER.writeLong(entryBuilder, 3);
                blockBuilder.closeEntry();
                type.writeObject(outputBlock, type.getObject(blockBuilder, 0));
            }
            else {
                outputBlock.appendNull();
            }
        }
    }
}
