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
package com.facebook.presto.metadata;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty;
import com.facebook.presto.spi.function.InvocationConvention;
import com.facebook.presto.spi.function.InvocationConvention.InvocationReturnConvention;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.metadata.FunctionInvokerProvider.checkChoice;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentType.VALUE_TYPE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.BLOCK_AND_POSITION;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.USE_BOXED_TYPE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.USE_NULL_FLAG;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestFunctionInvokerProvider
        extends AbstractTestFunctions
{
    @Test
    public void testFunctionInvokerProvider()
    {
        assertTrue(checkChoice(
                ImmutableList.of(
                        new ArgumentProperty(VALUE_TYPE, Optional.of(USE_BOXED_TYPE), Optional.empty()),
                        new ArgumentProperty(VALUE_TYPE, Optional.of(USE_BOXED_TYPE), Optional.empty())),
                true,
                false,
                Optional.of(new InvocationConvention(ImmutableList.of(BOXED_NULLABLE, BOXED_NULLABLE), InvocationReturnConvention.NULLABLE_RETURN, false))));

        assertTrue(checkChoice(
                ImmutableList.of(
                        new ArgumentProperty(VALUE_TYPE, Optional.of(RETURN_NULL_ON_NULL), Optional.empty()),
                        new ArgumentProperty(VALUE_TYPE, Optional.of(BLOCK_AND_POSITION), Optional.empty()),
                        new ArgumentProperty(VALUE_TYPE, Optional.of(BLOCK_AND_POSITION), Optional.empty())),
                false,
                false,
                Optional.of(new InvocationConvention(ImmutableList.of(NEVER_NULL, BLOCK_POSITION, BLOCK_POSITION), InvocationReturnConvention.FAIL_ON_NULL, false))));

        assertTrue(checkChoice(
                ImmutableList.of(
                        new ArgumentProperty(VALUE_TYPE, Optional.of(BLOCK_AND_POSITION), Optional.empty()),
                        new ArgumentProperty(VALUE_TYPE, Optional.of(USE_NULL_FLAG), Optional.empty()),
                        new ArgumentProperty(VALUE_TYPE, Optional.of(BLOCK_AND_POSITION), Optional.empty())),
                false,
                false,
                Optional.of(new InvocationConvention(ImmutableList.of(BLOCK_POSITION, NULL_FLAG, BLOCK_POSITION), InvocationReturnConvention.FAIL_ON_NULL, false))));

        assertFalse(checkChoice(
                ImmutableList.of(
                        new ArgumentProperty(VALUE_TYPE, Optional.of(BLOCK_AND_POSITION), Optional.empty()),
                        new ArgumentProperty(VALUE_TYPE, Optional.of(USE_BOXED_TYPE), Optional.empty())),
                false,
                false,
                Optional.of(new InvocationConvention(ImmutableList.of(BLOCK_POSITION, BOXED_NULLABLE), InvocationReturnConvention.NULLABLE_RETURN, false))));

        assertFalse(checkChoice(
                ImmutableList.of(
                        new ArgumentProperty(VALUE_TYPE, Optional.of(BLOCK_AND_POSITION), Optional.empty()),
                        new ArgumentProperty(VALUE_TYPE, Optional.of(BLOCK_AND_POSITION), Optional.empty())),
                false,
                false,
                Optional.of(new InvocationConvention(ImmutableList.of(BLOCK_POSITION, NULL_FLAG), InvocationReturnConvention.NULLABLE_RETURN, false))));

        assertFalse(checkChoice(
                ImmutableList.of(
                        new ArgumentProperty(VALUE_TYPE, Optional.of(USE_NULL_FLAG), Optional.empty()),
                        new ArgumentProperty(VALUE_TYPE, Optional.of(USE_BOXED_TYPE), Optional.empty())),
                true,
                false,
                Optional.of(new InvocationConvention(ImmutableList.of(BLOCK_POSITION, BOXED_NULLABLE), InvocationReturnConvention.FAIL_ON_NULL, false))));
    }
}
