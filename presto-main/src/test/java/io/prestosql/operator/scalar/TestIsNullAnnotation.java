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
package io.prestosql.operator.scalar;

import io.airlift.slice.Slice;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.IsNull;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class TestIsNullAnnotation
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerScalar(getClass());
    }

    @ScalarFunction("test_is_null_simple")
    @SqlType(StandardTypes.BIGINT)
    public static long testIsNullSimple(@SqlType(StandardTypes.BIGINT) long value, @IsNull boolean isNull)
    {
        if (isNull) {
            return 100;
        }
        return 2 * value;
    }

    @ScalarFunction("test_is_null")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice testIsNull(
            ConnectorSession session,
            @SqlType(StandardTypes.INTEGER) long longValue,
            @IsNull boolean isNullLong,
            @SqlType(StandardTypes.VARCHAR) Slice varcharNotNullable,
            @SqlType(StandardTypes.VARCHAR)
            @SqlNullable Slice varcharNullable,
            @SqlType(StandardTypes.VARCHAR) Slice varcharIsNull,
            @IsNull boolean isNullVarchar)
    {
        checkArgument(session != null, "session is null");

        StringBuilder builder = new StringBuilder();

        if (!isNullLong) {
            builder.append(longValue);
        }
        builder.append(":");

        checkArgument(varcharNotNullable != null, "varcharNotNullable is null while it doesn't has @SqlNullable");
        builder.append(varcharNotNullable.toStringUtf8())
                .append(":");

        if (varcharNullable != null) {
            builder.append(varcharNullable.toStringUtf8());
        }
        builder.append(":");

        if (!isNullVarchar) {
            builder.append(varcharIsNull.toStringUtf8());
        }
        return utf8Slice(builder.toString());
    }

    @ScalarFunction("test_is_null_void")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean testIsNullVoid(@SqlType("unknown") boolean value, @IsNull boolean isNull)
    {
        return isNull;
    }

    @Test
    public void testIsNull()
    {
        assertFunction("test_is_null_simple(-100)", BIGINT, -200L);
        assertFunction("test_is_null_simple(23)", BIGINT, 46L);
        assertFunction("test_is_null_simple(null)", BIGINT, 100L);
        assertFunction("test_is_null_simple(cast(null as bigint))", BIGINT, 100L);

        assertFunction("test_is_null(23, 'aaa', 'bbb', 'ccc')", VARCHAR, "23:aaa:bbb:ccc");
        assertFunction("test_is_null(null, 'aaa', 'bbb', 'ccc')", VARCHAR, ":aaa:bbb:ccc");
        assertFunction("test_is_null(null, 'aaa', null, 'ccc')", VARCHAR, ":aaa::ccc");
        assertFunction("test_is_null(23, 'aaa', null, null)", VARCHAR, "23:aaa::");
        assertFunction("test_is_null(23, null, 'bbb', 'ccc')", VARCHAR, null);

        assertFunction("test_is_null_void(null)", BOOLEAN, true);
    }
}
