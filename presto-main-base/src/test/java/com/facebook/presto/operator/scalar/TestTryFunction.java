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

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Locale;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

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
        assertFunction("\"$internal$try\"(() -> DOUBLE '4.5')", DOUBLE, 4.5);
        assertFunction("\"$internal$try\"(() -> DECIMAL '4.5')", createDecimalType(2, 1), SqlDecimal.of("4.5"));
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

    @Test
    public void testErrorNotCatchableByDefault()
    {
        // Custom error codes should NOT be catchable by TRY by default
        ErrorCode customError = new ErrorCode(0x0005_0001, "CUSTOM_ERROR", ErrorType.EXTERNAL);
        PrestoException customException = new PrestoException(() -> customError, "Custom error");

        SqlFunctionProperties propertiesEmpty = SqlFunctionProperties.builder()
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .setSessionStartTime(System.currentTimeMillis())
                .setSessionLocale(Locale.ENGLISH)
                .setSessionUser("test")
                .setTryCatchableErrorCodes(ImmutableSet.of())
                .build();

        try {
            TryFunction.tryLong(propertiesEmpty, () -> {
                throw customException;
            });
            fail("Expected PrestoException to be thrown when error code is not in catchable list");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), customError);
        }
    }

    @Test
    public void testErrorCatchableWithSessionProperty()
    {
        // Errors should be catchable by TRY when error code name is in the session property list
        ErrorCode customError = new ErrorCode(0x0005_0001, "CUSTOM_ERROR", ErrorType.EXTERNAL);
        PrestoException customException = new PrestoException(() -> customError, "Custom error");

        SqlFunctionProperties propertiesWithError = SqlFunctionProperties.builder()
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .setSessionStartTime(System.currentTimeMillis())
                .setSessionLocale(Locale.ENGLISH)
                .setSessionUser("test")
                .setTryCatchableErrorCodes(ImmutableSet.of("CUSTOM_ERROR"))
                .build();

        Long result = TryFunction.tryLong(propertiesWithError, () -> {
            throw customException;
        });
        assertNull(result, "Custom error should be caught when error code is in catchable list");
    }

    @Test
    public void testMultipleErrorsCatchableWithSessionProperty()
    {
        // Multiple error codes can be specified in the session property
        ErrorCode error1 = new ErrorCode(0x0005_0001, "ERROR_ONE", ErrorType.EXTERNAL);
        ErrorCode error2 = new ErrorCode(0x0005_0002, "ERROR_TWO", ErrorType.EXTERNAL);
        ErrorCode error3 = new ErrorCode(0x0005_0003, "ERROR_THREE", ErrorType.EXTERNAL);

        SqlFunctionProperties propertiesWithMultiple = SqlFunctionProperties.builder()
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .setSessionStartTime(System.currentTimeMillis())
                .setSessionLocale(Locale.ENGLISH)
                .setSessionUser("test")
                .setTryCatchableErrorCodes(ImmutableSet.of("ERROR_ONE", "ERROR_TWO"))
                .build();

        // ERROR_ONE should be caught
        PrestoException exception1 = new PrestoException(() -> error1, "Error one");
        Long result1 = TryFunction.tryLong(propertiesWithMultiple, () -> {
            throw exception1;
        });
        assertNull(result1, "ERROR_ONE should be caught when in catchable list");

        // ERROR_TWO should be caught
        PrestoException exception2 = new PrestoException(() -> error2, "Error two");
        Long result2 = TryFunction.tryLong(propertiesWithMultiple, () -> {
            throw exception2;
        });
        assertNull(result2, "ERROR_TWO should be caught when in catchable list");

        // ERROR_THREE should NOT be caught (not in list)
        PrestoException exception3 = new PrestoException(() -> error3, "Error three");
        try {
            TryFunction.tryLong(propertiesWithMultiple, () -> {
                throw exception3;
            });
            fail("Expected PrestoException for ERROR_THREE which is not in catchable list");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), error3);
        }
    }

    @Test
    public void testDefaultCatchableByTryStillWorks()
    {
        // Errors marked with catchableByTry=true in their definition should still be caught
        // even without session property (e.g., DIVISION_BY_ZERO)
        SqlFunctionProperties propertiesEmpty = SqlFunctionProperties.builder()
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .setSessionStartTime(System.currentTimeMillis())
                .setSessionLocale(Locale.ENGLISH)
                .setSessionUser("test")
                .setTryCatchableErrorCodes(ImmutableSet.of())
                .build();

        // This test uses the actual TRY function behavior through assertFunction
        // DIVISION_BY_ZERO is marked catchableByTry=true in StandardErrorCode
        assertFunction("\"$internal$try\"(() -> 1/0)", INTEGER, null);
    }
}
