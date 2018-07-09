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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ScalarImplementationChoice;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.BLOCK_AND_POSITION;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.type.TestBlockAndPositionNullConvention.FunctionWithBlockAndPositionConvention.BLOCK_AND_POSITION_CONVENTION;
import static com.facebook.presto.util.Reflection.methodHandle;
import static org.testng.Assert.assertTrue;

public class TestBlockAndPositionNullConvention
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerScalarFunction(BLOCK_AND_POSITION_CONVENTION);
    }

    @Test
    public void testBlockPosition()
    {
        assertFunction("identityFunction(9876543210)", BIGINT, 9876543210L);
        assertFunction("identityFunction(bound_long)", BIGINT, 1234L);
        assertTrue(FunctionWithBlockAndPositionConvention.hitBlockPosition.get());
    }

    public static class FunctionWithBlockAndPositionConvention
            extends SqlScalarFunction
    {
        private static final AtomicBoolean hitBlockPosition = new AtomicBoolean();
        public static final FunctionWithBlockAndPositionConvention BLOCK_AND_POSITION_CONVENTION = new FunctionWithBlockAndPositionConvention();

        private static final MethodHandle METHOD_HANDLE_BLOCK_AND_POSITION = methodHandle(FunctionWithBlockAndPositionConvention.class, "getBlockPosition", Block.class, int.class);
        private static final MethodHandle METHOD_HANDLE_NULL_ON_NULL = methodHandle(FunctionWithBlockAndPositionConvention.class, "getLong", long.class);

        protected FunctionWithBlockAndPositionConvention()
        {
            super(new Signature("identityFunction", FunctionKind.SCALAR, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
        }

        @Override
        public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            return new ScalarFunctionImplementation(
                    ImmutableList.of(
                            new ScalarImplementationChoice(
                                    false,
                                    ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                                    METHOD_HANDLE_NULL_ON_NULL,
                                    Optional.empty()),
                            new ScalarImplementationChoice(
                                    false,
                                    ImmutableList.of(valueTypeArgumentProperty(BLOCK_AND_POSITION)),
                                    METHOD_HANDLE_BLOCK_AND_POSITION,
                                    Optional.empty())),
                    isDeterministic());
        }

        public static long getBlockPosition(Block block, int position)
        {
            hitBlockPosition.set(true);
            return BIGINT.getLong(block, position);
        }

        public static long getLong(long number)
        {
            return number;
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
