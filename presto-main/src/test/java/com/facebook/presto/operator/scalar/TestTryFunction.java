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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.ArrayType;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static java.util.Arrays.asList;

public class TestTryFunction
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerScalar(getClass());
    }

    @ScalarFunction
    @SqlType("bigint")
    public static long throwError()
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "internal error, should not be suppressed by $internal$try");
    }

    @Test
    public void testBasic()
    {
        assertFunction("\"$internal$try\"(() -> 42)", INTEGER, 42);
        assertFunction("\"$internal$try\"(() -> 4.5)", DOUBLE, 4.5);
        assertFunction("\"$internal$try\"(() -> TRUE)", BOOLEAN, true);
        assertFunction("\"$internal$try\"(() -> 'hello')", createVarcharType(5), "hello");
        assertFunction("\"$internal$try\"(() -> JSON '[true, false, 12, 12.7, \"12\", null]')", JSON, "[true,false,12,12.7,\"12\",null]");
        assertFunction("\"$internal$try\"(() -> ARRAY [1, 2])", new ArrayType(INTEGER), asList(1, 2));
        assertFunction("\"$internal$try\"(() -> NULL)", UNKNOWN, null);
    }

    @Test
    public void testExceptions()
    {
        // Exceptions that should be suppressed
        assertFunction("\"$internal$try\"(() -> 1/0)", INTEGER, null);
        assertFunction("\"$internal$try\"(() -> JSON_PARSE('INVALID'))", JSON, null);
        assertFunction("\"$internal$try\"(() -> CAST(NULL AS INTEGER))", INTEGER, null);
        assertFunction("\"$internal$try\"(() -> ABS(-9223372036854775807 - 1))", BIGINT, null);

        // Exceptions that should not be suppressed
        assertInvalidFunction("\"$internal$try\"(() -> throw_error())", GENERIC_INTERNAL_ERROR);
    }
}
