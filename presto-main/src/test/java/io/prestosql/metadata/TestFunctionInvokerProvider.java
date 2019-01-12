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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableList;
import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.metadata.FunctionInvokerProvider.checkChoice;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentType.VALUE_TYPE;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.BLOCK_AND_POSITION;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_NULL_FLAG;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
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
