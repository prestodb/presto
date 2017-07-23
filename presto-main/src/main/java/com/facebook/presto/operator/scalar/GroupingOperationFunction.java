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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.StandardTypes;

import static com.facebook.presto.operator.scalar.ArrayElementAtFunction.longElementAt;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;

/**
 * The grouping function is used in conjunction with GROUPING SETS, ROLLUP and CUBE to
 * indicate which columns are present in that grouping. Since all that information is
 * known during planning, that is when results are computed and populated in an array.
 * At runtime we defer to the "element_at" function and lookup the pre-computed values
 * based on the groupId index.
 */
public final class GroupingOperationFunction
{
    public static final String BIGINT_GROUPING = "bigint_grouping";
    public static final String INTEGER_GROUPING = "integer_grouping";
    public static final int MAX_NUMBER_GROUPING_ARGUMENTS_BIGINT = 63;
    public static final int MAX_NUMBER_GROUPING_ARGUMENTS_INTEGER = 31;

    private GroupingOperationFunction() {}

    @ScalarFunction(value = INTEGER_GROUPING, deterministic = false)
    @SqlType(StandardTypes.INTEGER)
    public static long integerGrouping(
            @SqlType("array(integer)") Block array,
            @SqlType(BIGINT) long groupId)
    {
        return longElementAt(IntegerType.INTEGER, array, groupId);
    }

    @ScalarFunction(value = BIGINT_GROUPING, deterministic = false)
    @SqlType(StandardTypes.BIGINT)
    public static long bigintGrouping(
            @SqlType("array(bigint)") Block array,
            @SqlType(BIGINT) long groupId)
    {
        return longElementAt(BigintType.BIGINT, array, groupId);
    }
}
