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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;

public class TestArrayCumulativeSumFunction
        extends AbstractTestFunctions
{
    @Test
    public void testIntArray()
    {
        assertFunction("array_cum_sum(ARRAY [cast(5 as INTEGER), 6, 0])", new ArrayType(INTEGER), ImmutableList.of(5, 11, 11));
        assertFunction("array_cum_sum(ARRAY [cast(5 as INTEGER), 6, null, null, 2, 1])", new ArrayType(INTEGER), Arrays.asList(5, 11, null, null, null, null));
        assertFunction("array_cum_sum(ARRAY [cast(null as INTEGER), 6, 2, 3, 2, 1])", new ArrayType(INTEGER), Arrays.asList(null, null, null, null, null, null));
        assertFunction("array_cum_sum(CAST(ARRAY[] AS array(integer)))", new ArrayType(INTEGER), ImmutableList.of());
        assertFunction("array_cum_sum(CAST(NULL AS array(integer)))", new ArrayType(INTEGER), null);
        assertFunctionThrowsIncorrectly("array_cum_sum(ARRAY [cast(2147483647 as integer), 2147483647, 2147483647])", PrestoException.class, "integer addition overflow:.*");
    }

    @Test
    public void testBigIntArray()
    {
        assertFunction("array_cum_sum(ARRAY [cast(5 as bigint), 6, 0])", new ArrayType(BIGINT), ImmutableList.of(5L, 11L, 11L));
        assertFunction("array_cum_sum(ARRAY [cast(5 as bigint), 6, null, null, 2, 1])", new ArrayType(BIGINT), Arrays.asList(5L, 11L, null, null, null, null));
        assertFunction("array_cum_sum(ARRAY [cast(null as bigint), 6, 2, 3, 2, 1])", new ArrayType(BIGINT), Arrays.asList(null, null, null, null, null, null));
        assertFunction("array_cum_sum(CAST(ARRAY[] AS array(bigint)))", new ArrayType(BIGINT), ImmutableList.of());
        assertFunction("array_cum_sum(CAST(NULL AS array(bigint)))", new ArrayType(BIGINT), null);
        assertFunction("array_cum_sum(ARRAY [cast(2147483647 as bigint), 2147483647, 2147483647])", new ArrayType(BIGINT), ImmutableList.of(2147483647L, 4294967294L, 6442450941L));
    }

    @Test
    public void testRealArray()
    {
        assertFunctionFloatArrayWithError("array_cum_sum(ARRAY [cast(5.1 as real), 6.1, 0.5])", new ArrayType(REAL), ImmutableList.of(5.1f, 11.2f, 11.7f), 1e-5f);
        assertFunction("array_cum_sum(ARRAY [cast(5.1 as real), 6, null, null, 2, 1.2])", new ArrayType(REAL), Arrays.asList(5.1f, 11.1f, null, null, null, null));
        assertFunction("array_cum_sum(ARRAY [cast(null as real), 6.2, 2, 3, 2, 1])", new ArrayType(REAL), Arrays.asList(null, null, null, null, null, null));
        assertFunction("array_cum_sum(CAST(ARRAY[] AS array(real)))", new ArrayType(REAL), ImmutableList.of());
        assertFunction("array_cum_sum(CAST(NULL AS array(real)))", new ArrayType(REAL), null);
    }

    @Test
    public void testDoubleArray()
    {
        assertFunctionDoubleArrayWithError("array_cum_sum(ARRAY [cast(5.1 as double), 6.1, 0.5])", new ArrayType(DOUBLE), ImmutableList.of(5.1, 11.2, 11.7), 1e-5);
        assertFunction("array_cum_sum(ARRAY [cast(5.1 as double), 6, null, null, 2, 1.2])", new ArrayType(DOUBLE), Arrays.asList(5.1, 11.1, null, null, null, null));
        assertFunction("array_cum_sum(ARRAY [cast(null as double), 6.2, 2, 3, 2, 1])", new ArrayType(DOUBLE), Arrays.asList(null, null, null, null, null, null));
        assertFunction("array_cum_sum(CAST(ARRAY[] AS array(double)))", new ArrayType(DOUBLE), ImmutableList.of());
        assertFunction("array_cum_sum(CAST(NULL AS array(double)))", new ArrayType(DOUBLE), null);
    }

    @Test
    public void testVarcharArray()
    {
        assertFunctionThrowsIncorrectly("array_cum_sum(ARRAY [cast(5.1 as varchar), '6.1', '0.5'])", OperatorNotFoundException.class, ".*cannot be applied to.*");
        assertFunctionThrowsIncorrectly("array_cum_sum(ARRAY [cast(null as varchar), '6.1', '0.5'])", OperatorNotFoundException.class, ".*cannot be applied to.*");
    }

    @Test
    public void testDecimalArray()
    {
        assertFunction("array_cum_sum(cast(ARRAY[5.1, 6.1, 0.5] as array(decimal(2, 1))))", new ArrayType(createDecimalType(2, 1)),
                ImmutableList.of(SqlDecimal.of(51, 2, 1), SqlDecimal.of(112, 2, 1), SqlDecimal.of(117, 2, 1)));
        assertFunction("array_cum_sum(cast(ARRAY[5.1, 6, null, null, 2, 1.2] as array(decimal(2, 1))))", new ArrayType(createDecimalType(2, 1)),
                Arrays.asList(SqlDecimal.of(51, 2, 1), SqlDecimal.of(111, 2, 1), null, null, null, null));
        assertFunction("array_cum_sum(cast(ARRAY[null, 6, 2, 3, 2, 1.2] as array(decimal(2, 1))))", new ArrayType(createDecimalType(2, 1)), Arrays.asList(null, null, null, null, null, null));
    }
}
