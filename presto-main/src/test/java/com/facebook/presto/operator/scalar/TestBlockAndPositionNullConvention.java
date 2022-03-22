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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBlockAndPositionNullConvention
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerParametricScalar(FunctionWithBlockAndPositionConvention.class);
    }

    @Test
    public void testBlockPosition()
    {
        assertFunction("test_block_position(9876543210)", BIGINT, 9876543210L);
        assertFalse(FunctionWithBlockAndPositionConvention.hitBlockPositionBigint.get());

        assertFunction("test_block_position(bound_long)", BIGINT, 1234L);
        assertTrue(FunctionWithBlockAndPositionConvention.hitBlockPositionBigint.get());

        assertFunction("test_block_position(3.0E0)", DOUBLE, 3.0);
        assertFalse(FunctionWithBlockAndPositionConvention.hitBlockPositionDouble.get());

        assertFunction("test_block_position(bound_double)", DOUBLE, 12.34);
        assertTrue(FunctionWithBlockAndPositionConvention.hitBlockPositionDouble.get());

        assertFunction("test_block_position(bound_string)", VARCHAR, "hello");
        assertTrue(FunctionWithBlockAndPositionConvention.hitBlockPositionSlice.get());

        // TODO: add adaptations so these will pass
        //assertFunction("test_block_position(null)", UNKNOWN, null);
        //assertFalse(FunctionWithBlockAndPositionConvention.hitBlockPositionObject.get());

        assertFunction("test_block_position(false)", BOOLEAN, false);
        assertFalse(FunctionWithBlockAndPositionConvention.hitBlockPositionBoolean.get());

        assertFunction("test_block_position(bound_boolean)", BOOLEAN, true);
        assertTrue(FunctionWithBlockAndPositionConvention.hitBlockPositionBoolean.get());
    }

    @ScalarFunction(value = "test_block_position", calledOnNullInput = true)
    public static class FunctionWithBlockAndPositionConvention
    {
        private static final AtomicBoolean hitBlockPositionBigint = new AtomicBoolean();
        private static final AtomicBoolean hitBlockPositionDouble = new AtomicBoolean();
        private static final AtomicBoolean hitBlockPositionSlice = new AtomicBoolean();
        private static final AtomicBoolean hitBlockPositionBoolean = new AtomicBoolean();
        private static final AtomicBoolean hitBlockPositionObject = new AtomicBoolean();

        // generic implementations
        // these will not work right now because MethodHandle is not properly adapted

        @TypeParameter("E")
        @SqlNullable
        @SqlType("E")
        public static Object generic(@TypeParameter("E") Type type, @SqlNullable @SqlType("E") Object object)
        {
            return object;
        }

        @TypeParameter("E")
        @SqlNullable
        @SqlType("E")
        public static Object generic(@TypeParameter("E") Type type, @BlockPosition @SqlType("E") Block block, @BlockIndex int position)
        {
            hitBlockPositionObject.set(true);
            return readNativeValue(type, block, position);
        }

        // specialized

        @TypeParameter("E")
        @SqlNullable
        @SqlType("E")
        public static Slice specializedSlice(@TypeParameter("E") Type type, @SqlNullable @SqlType("E") Slice slice)
        {
            return slice;
        }

        @TypeParameter("E")
        @SqlType("E")
        public static Slice specializedSlice(@TypeParameter("E") Type type, @BlockPosition @SqlType(value = "E", nativeContainerType = Slice.class) Block block, @BlockIndex int position)
        {
            hitBlockPositionSlice.set(true);
            return type.getSlice(block, position);
        }

        @TypeParameter("E")
        @SqlNullable
        @SqlType("E")
        public static Boolean speciailizedBoolean(@TypeParameter("E") Type type, @SqlNullable @SqlType("E") Boolean bool)
        {
            return bool;
        }

        @TypeParameter("E")
        @SqlNullable
        @SqlType("E")
        public static Boolean speciailizedBoolean(@TypeParameter("E") Type type, @BlockPosition @SqlType(value = "E", nativeContainerType = boolean.class) Block block, @BlockIndex int position)
        {
            hitBlockPositionBoolean.set(true);
            return type.getBoolean(block, position);
        }

        // exact

        @SqlType(StandardTypes.BIGINT)
        public static long getLong(@SqlNullable @SqlType(StandardTypes.BIGINT) Long number)
        {
            return number;
        }

        @SqlType(StandardTypes.BIGINT)
        public static long getBlockPosition(@BlockPosition @SqlType(value = StandardTypes.BIGINT, nativeContainerType = long.class) Block block, @BlockIndex int position)
        {
            hitBlockPositionBigint.set(true);
            return BIGINT.getLong(block, position);
        }

        @SqlType(StandardTypes.DOUBLE)
        @SqlNullable
        public static Double getDouble(@SqlNullable @SqlType(StandardTypes.DOUBLE) Double number)
        {
            return number;
        }

        @SqlType(StandardTypes.DOUBLE)
        @SqlNullable
        public static Double getDouble(@BlockPosition @SqlType(value = StandardTypes.DOUBLE, nativeContainerType = double.class) Block block, @BlockIndex int position)
        {
            hitBlockPositionDouble.set(true);
            return DOUBLE.getDouble(block, position);
        }
    }
}
