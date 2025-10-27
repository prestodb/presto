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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

public final class ClpFunctions
{
    private ClpFunctions()
    {
    }

    @ScalarFunction(value = "CLP_GET_BIGINT", deterministic = false)
    @Description("Retrieves an integer value corresponding to the given JSON path.")
    @SqlType(StandardTypes.BIGINT)
    public static long clpGetBigint(@SqlType(StandardTypes.VARCHAR) Slice jsonPath)
    {
        throw new UnsupportedOperationException("CLP_GET_BIGINT is a placeholder function without implementation.");
    }

    @ScalarFunction(value = "CLP_GET_DOUBLE", deterministic = false)
    @Description("Retrieves a floating point value corresponding to the given JSON path.")
    @SqlType(StandardTypes.DOUBLE)
    public static double clpGetDouble(@SqlType(StandardTypes.VARCHAR) Slice jsonPath)
    {
        throw new UnsupportedOperationException("CLP_GET_DOUBLE is a placeholder function without implementation.");
    }

    @ScalarFunction(value = "CLP_GET_BOOL", deterministic = false)
    @Description("Retrieves a boolean value corresponding to the given JSON path.")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean clpGetBool(@SqlType(StandardTypes.VARCHAR) Slice jsonPath)
    {
        throw new UnsupportedOperationException("CLP_GET_BOOL is a placeholder function without implementation.");
    }

    @ScalarFunction(value = "CLP_GET_STRING", deterministic = false)
    @Description("Retrieves a string value corresponding to the given JSON path.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice clpGetString(@SqlType(StandardTypes.VARCHAR) Slice jsonPath)
    {
        throw new UnsupportedOperationException("CLP_GET_STRING is a placeholder function without implementation.");
    }

    @ScalarFunction(value = "CLP_GET_STRING_ARRAY", deterministic = false)
    @Description("Retrieves an array value corresponding to the given JSON path and converts each element into a string.")
    @SqlType("ARRAY(VARCHAR)")
    public static Block clpGetStringArray(@SqlType(StandardTypes.VARCHAR) Slice jsonPath)
    {
        throw new UnsupportedOperationException("CLP_GET_STRING_ARRAY is a placeholder function without implementation.");
    }

    @ScalarFunction(value = "CLP_WILDCARD_STRING_COLUMN", deterministic = false)
    @Description("Used in filter expressions to allow comparisons with any string column in the log record.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice clpWildcardStringColumn()
    {
        throw new UnsupportedOperationException("CLP_WILDCARD_STRING_COLUMN is a placeholder function without implementation.");
    }

    @ScalarFunction(value = "CLP_WILDCARD_INT_COLUMN", deterministic = false)
    @Description("Used in filter expressions to allow comparisons with any integer column in the log record.")
    @SqlType(StandardTypes.BIGINT)
    public static long clpWildcardIntColumn()
    {
        throw new UnsupportedOperationException("CLP_WILDCARD_INT_COLUMN is a placeholder function without implementation.");
    }

    @ScalarFunction(value = "CLP_WILDCARD_FLOAT_COLUMN", deterministic = false)
    @Description("Used in filter expressions to allow comparisons with any floating point column in the log record.")
    @SqlType(StandardTypes.DOUBLE)
    public static double clpWildcardFloatColumn()
    {
        throw new UnsupportedOperationException("CLP_WILDCARD_FLOAT_COLUMN is a placeholder function without implementation.");
    }

    @ScalarFunction(value = "CLP_WILDCARD_BOOL_COLUMN", deterministic = false)
    @Description("Used in filter expressions to allow comparisons with any boolean column in the log record.")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean clpWildcardBoolColumn()
    {
        throw new UnsupportedOperationException("CLP_WILDCARD_BOOL_COLUMN is a placeholder function without implementation.");
    }

    @ScalarFunction(value = "CLP_GET_JSON_STRING", deterministic = false)
    @Description("Converts an entire log record into a JSON string.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice clpGetJSONString()
    {
        throw new UnsupportedOperationException("CLP_GET_JSON_STRING is a placeholder function without implementation.");
    }
}
