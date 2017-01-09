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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import org.postgresql.util.PGobject;

import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;

public class PostgreSqlJsonFunctions
{
    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType("pg_json") Object left, @SqlType("pg_json") Object right)
    {
        return left.equals(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType("pg_json") Object left, @SqlType("pg_json") Object right)
    {
        return !left.equals(right);
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType("pg_json") Object value)
    {
        return ((PGobject) value).getValue().hashCode();
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equalB(@SqlType("pg_jsonb") Object left, @SqlType("pg_jsonb") Object right)
    {
        return left.equals(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqualB(@SqlType("pg_jsonb") Object left, @SqlType("pg_jsonb") Object right)
    {
        return !left.equals(right);
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCodeB(@SqlType("pg_jsonb") Object value)
    {
        return ((PGobject) value).getValue().hashCode();
    }

    private PostgreSqlJsonFunctions() {}
}
