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

package com.facebook.presto.scalar.sql;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.SqlInvokedScalarFunction;
import com.facebook.presto.spi.function.SqlParameter;
import com.facebook.presto.spi.function.SqlParameters;
import com.facebook.presto.spi.function.SqlType;

public class StringSqlFunctions
{
    private StringSqlFunctions() {}

    @SqlInvokedScalarFunction(value = "replace_first", deterministic = true, calledOnNullInput = true)
    @Description("Replaces the first occurrence of a substring that matches the given pattern with the given replacement.")
    @SqlParameters({@SqlParameter(name = "str", type = "varchar"), @SqlParameter(name = "search", type = "varchar"), @SqlParameter(name = "replace", type = "varchar")})
    @SqlType("varchar")
    public static String replaceFirst()
    {
        return "RETURN IF(replace IS NULL, NULL, IF(STRPOS(str, search) = 0, str, SUBSTR(str, 1, STRPOS(str, search) - 1) || replace || SUBSTR(str, STRPOS(str, search) + LENGTH(search))))";
    }

    @SqlInvokedScalarFunction(value = "trail", deterministic = true, calledOnNullInput = true)
    @Description("Returns the last N characters of the input string.")
    @SqlParameters({@SqlParameter(name = "str", type = "varchar"), @SqlParameter(name = "N", type = "int")})
    @SqlType("varchar")
    public static String trail()
    {
        return "RETURN REVERSE(SUBSTR(REVERSE(str), 1, N))";
    }
}
