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

import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class TestLiteralParametersFunctions
        extends AbstractTestFunctions
{
    {
        registerScalar(TestLiteralParametersFunctions.class);
    }

    @Test
    public void testLiteralParametersFunction()
    {
        assertFunction("get_slice_length_from_literal('foo')", BIGINT, 3);
        assertFunction("many_literal_parameters('foo', 'hello')", BIGINT, 3);
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long getSliceLengthFromLiteral(@Literal("x") long x, @SqlType("varchar(x)") Slice str)
    {
        return x;
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long manyLiteralParameters(@Literal("x") long x, @SqlType("varchar(x)") Slice str1, @Literal("x") long y, @SqlType("varchar(y)") Slice str2)
    {
        return x;
    }
}
