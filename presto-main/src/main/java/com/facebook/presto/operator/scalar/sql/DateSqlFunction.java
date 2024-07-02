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
package com.facebook.presto.operator.scalar.sql;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.SqlInvokedScalarFunction;
import com.facebook.presto.spi.function.SqlParameter;
import com.facebook.presto.spi.function.SqlParameters;
import com.facebook.presto.spi.function.SqlType;

public class DateSqlFunction
{
    private DateSqlFunction() {}

    @SqlInvokedScalarFunction(value = "date_diff", deterministic = true, calledOnNullInput = false)
    @Description("Difference of the given dates in the given unit")
    @SqlParameters({@SqlParameter(name = "unit", type = "varchar"), @SqlParameter(name = "unixtime1", type = "bigint"), @SqlParameter(name = "unixtime2", type = "bigint")})
    @SqlType("bigint")
    public static String diffUnixtime()
    {
        return "RETURN IF(unit NOT IN ('millisecond', 'month', 'quarter', 'year'), " +
                "CASE WHEN unit = 'week' THEN (unixtime2 - unixtime1) / 604800 " +
                "WHEN unit = 'day' THEN (unixtime2 - unixtime1) / 86400 " +
                "WHEN unit = 'hour' THEN (unixtime2 - unixtime1) / 3600 " +
                "WHEN unit = 'minute' THEN (unixtime2 - unixtime1) / 60 " +
                "WHEN unit = 'second' THEN unixtime2 - unixtime1 END, " +
                "fail(7, 'Invalid unix time parameters'))";
    }
}
