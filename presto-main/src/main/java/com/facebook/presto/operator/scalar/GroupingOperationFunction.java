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

import com.facebook.presto.operator.Description;
import com.facebook.presto.type.LiteralParameters;
import com.facebook.presto.type.SqlType;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.StandardTypes.BIGINT;

public final class GroupingOperationFunction
{
    private GroupingOperationFunction() {}

    private static final Logger log = Logger.get(GroupingOperationFunction.class);

    @ScalarFunction
    @Description("Returns a bitmap as an integer, indicating which columns are present in the grouping")
    @LiteralParameters("x")
    @SqlType(BIGINT)
    public static long grouping(@SqlType(BIGINT) long groupId, @SqlType("varchar(x)") Slice groupingColumns)
    {
        log.debug(groupId + " " + new String(groupingColumns.getBytes()));
        return groupId;
    }
}
