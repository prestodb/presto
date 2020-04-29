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
package com.facebook.presto.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.Convention;
import com.facebook.presto.spi.function.FunctionDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static com.google.common.base.Throwables.throwIfInstanceOf;

public class TestConventionDependencies
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerParametricScalar(RegularConvention.class);
        registerParametricScalar(BlockPositionConvention.class);
        registerParametricScalar(Add.class);
    }

    @Test
    public void testConventionDependencies()
    {
        assertFunction("regular_convention(1, 1)", INTEGER, 2);
        assertFunction("regular_convention(50, 10)", INTEGER, 60);
        assertFunction("regular_convention(1, 0)", INTEGER, 1);
        assertFunction("block_position_convention(ARRAY [1, 2, 3])", INTEGER, 6);
        assertFunction("block_position_convention(ARRAY [25, 0, 5])", INTEGER, 30);
        assertFunction("block_position_convention(ARRAY [56, 275, 36])", INTEGER, 367);
    }

    @ScalarFunction(value = "regular_convention")
    public static class RegularConvention
    {
        @SqlType(StandardTypes.INTEGER)
        public static long testRegularConvention(
                @FunctionDependency(name = "add",
                        returnType = StandardTypes.INTEGER,
                        argumentTypes = {StandardTypes.INTEGER, StandardTypes.INTEGER},
                        convention = @Convention(arguments = {NEVER_NULL, BOXED_NULLABLE}, result = FAIL_ON_NULL)) MethodHandle function,
                @SqlType(StandardTypes.INTEGER) long left,
                @SqlType(StandardTypes.INTEGER) long right)
        {
            try {
                return (long) function.invokeExact(left, Long.valueOf(right));
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }

    @ScalarFunction("block_position_convention")
    public static class BlockPositionConvention
    {
        @SqlType(StandardTypes.INTEGER)
        public static long testBlockPositionConvention(
                @FunctionDependency(
                        name = "add",
                        returnType = StandardTypes.INTEGER,
                        argumentTypes = {StandardTypes.INTEGER, StandardTypes.INTEGER},
                        convention = @Convention(arguments = {NEVER_NULL, BLOCK_POSITION}, result = FAIL_ON_NULL)) MethodHandle function,
                @SqlType("array(int)") Block array)
        {
            long sum = 0;
            for (int i = 0; i < array.getPositionCount(); i++) {
                try {
                    sum = (long) function.invokeExact(sum, array, i);
                }
                catch (Throwable t) {
                    throwIfInstanceOf(t, Error.class);
                    throwIfInstanceOf(t, PrestoException.class);
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
                }
            }
            return sum;
        }
    }

    @ScalarFunction(value = "add", calledOnNullInput = true)
    public static class Add
    {
        @SqlType(StandardTypes.INTEGER)
        public static long add(
                @SqlType(StandardTypes.INTEGER) long left,
                @SqlNullable @SqlType(StandardTypes.INTEGER) Long right)
        {
            return Math.addExact((int) left, (int) right.longValue());
        }

        @SqlType(StandardTypes.INTEGER)
        public static long addBlockPosition(
                @SqlType(StandardTypes.INTEGER) long first,
                @BlockPosition @SqlType(value = StandardTypes.INTEGER, nativeContainerType = long.class) Block block,
                @BlockIndex int position)
        {
            return Math.addExact((int) first, (int) INTEGER.getLong(block, position));
        }
    }
}
