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
package io.prestosql.type;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.BlockIndex;
import io.prestosql.spi.function.BlockPosition;
import io.prestosql.spi.function.Convention;
import io.prestosql.spi.function.FunctionDependency;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.type.IntegerType.INTEGER;

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

    @ScalarFunction("regular_convention")
    public static class RegularConvention
    {
        @SqlType(StandardTypes.INTEGER)
        public static long testRegularConvention(
                @FunctionDependency(name = "add",
                        returnType = StandardTypes.INTEGER,
                        argumentTypes = {StandardTypes.INTEGER, StandardTypes.INTEGER},
                        convention = @Convention(arguments = {NEVER_NULL, NEVER_NULL}, result = FAIL_ON_NULL)) MethodHandle function,
                @SqlType(StandardTypes.INTEGER) long left,
                @SqlType(StandardTypes.INTEGER) long right)
        {
            try {
                return (long) function.invokeExact(left, right);
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

    @ScalarFunction("add")
    public static class Add
    {
        @SqlType(StandardTypes.INTEGER)
        public static long add(
                @SqlType(StandardTypes.INTEGER) long left,
                @SqlType(StandardTypes.INTEGER) long right)
        {
            return Math.addExact((int) left, (int) right);
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
