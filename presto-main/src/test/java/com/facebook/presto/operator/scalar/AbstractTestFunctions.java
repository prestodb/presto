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

import com.facebook.presto.Session;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.DecimalParseResult;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public abstract class AbstractTestFunctions
{
    private static final double DELTA = 1e-5;

    protected final Session session;
    private final FeaturesConfig config;
    protected FunctionAssertions functionAssertions;

    protected AbstractTestFunctions()
    {
        this(TEST_SESSION);
    }

    protected AbstractTestFunctions(Session session)
    {
        this(session, new FeaturesConfig());
    }

    protected AbstractTestFunctions(FeaturesConfig config)
    {
        this(TEST_SESSION, config);
    }

    protected AbstractTestFunctions(Session session, FeaturesConfig config)
    {
        this.session = requireNonNull(session, "session is null");
        this.config = requireNonNull(config, "config is null").setLegacyLogFunction(true);
    }

    @BeforeClass
    public final void initTestFunctions()
    {
        functionAssertions = new FunctionAssertions(session, config);
    }

    @AfterClass(alwaysRun = true)
    public final void destroyTestFunctions()
    {
        closeAllRuntimeException(functionAssertions);
        functionAssertions = null;
    }

    protected void assertFunction(String projection, Type expectedType, Object expected)
    {
        functionAssertions.assertFunction(projection, expectedType, expected);
    }

    protected void assertFunctionWithError(String projection, Type expectedType, double expected)
    {
        assertFunctionWithError(projection, expectedType, expected, DELTA);
    }

    protected void assertFunctionWithError(String projection, Type expectedType, double expected, double delta)
    {
        functionAssertions.assertFunctionWithError(projection, expectedType, expected, delta);
    }

    protected void assertOperator(OperatorType operator, String value, Type expectedType, Object expected)
    {
        functionAssertions.assertFunction(format("\"%s\"(%s)", operator.getFunctionName().getObjectName(), value), expectedType, expected);
    }

    protected void assertDecimalFunction(String statement, SqlDecimal expectedResult)
    {
        assertFunction(
                statement,
                createDecimalType(expectedResult.getPrecision(), expectedResult.getScale()),
                expectedResult);
    }

    // this is not safe as it catches all RuntimeExceptions
    @Deprecated
    protected void assertInvalidFunction(String projection)
    {
        functionAssertions.assertInvalidFunction(projection);
    }

    protected void assertInvalidFunction(String projection, StandardErrorCode errorCode, String messagePattern)
    {
        functionAssertions.assertInvalidFunction(projection, errorCode, messagePattern);
    }

    protected void assertInvalidFunction(String projection, String messagePattern)
    {
        functionAssertions.assertInvalidFunction(projection, INVALID_FUNCTION_ARGUMENT, messagePattern);
    }

    protected void assertInvalidFunction(String projection, SemanticErrorCode expectedErrorCode)
    {
        functionAssertions.assertInvalidFunction(projection, expectedErrorCode);
    }

    protected void assertInvalidFunction(String projection, SemanticErrorCode expectedErrorCode, String message)
    {
        functionAssertions.assertInvalidFunction(projection, expectedErrorCode, message);
    }

    protected void assertInvalidFunction(String projection, ErrorCodeSupplier expectedErrorCode)
    {
        functionAssertions.assertInvalidFunction(projection, expectedErrorCode);
    }

    protected void assertNumericOverflow(String projection, String message)
    {
        functionAssertions.assertNumericOverflow(projection, message);
    }

    protected void assertInvalidCast(String projection)
    {
        functionAssertions.assertInvalidCast(projection);
    }

    protected void assertInvalidCast(String projection, String message)
    {
        functionAssertions.assertInvalidCast(projection, message);
    }

    public void assertCachedInstanceHasBoundedRetainedSize(String projection)
    {
        functionAssertions.assertCachedInstanceHasBoundedRetainedSize(projection);
    }

    protected void assertNotSupported(String projection, String message)
    {
        try {
            functionAssertions.executeProjectionWithFullEngine(projection);
            fail("expected exception");
        }
        catch (PrestoException e) {
            try {
                assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
                assertEquals(e.getMessage(), message);
            }
            catch (Throwable failure) {
                failure.addSuppressed(e);
                throw failure;
            }
        }
    }

    protected void tryEvaluateWithAll(String projection, Type expectedType)
    {
        functionAssertions.tryEvaluateWithAll(projection, expectedType);
    }

    protected void registerScalarFunction(SqlScalarFunction sqlScalarFunction)
    {
        Metadata metadata = functionAssertions.getMetadata();
        metadata.getFunctionAndTypeManager().registerBuiltInFunctions(ImmutableList.of(sqlScalarFunction));
    }

    protected void registerScalar(Class<?> clazz)
    {
        Metadata metadata = functionAssertions.getMetadata();
        List<SqlFunction> functions = new FunctionListBuilder()
                .scalars(clazz)
                .getFunctions();
        metadata.getFunctionAndTypeManager().registerBuiltInFunctions(functions);
    }

    protected void registerParametricScalar(Class<?> clazz)
    {
        Metadata metadata = functionAssertions.getMetadata();
        List<SqlFunction> functions = new FunctionListBuilder()
                .scalar(clazz)
                .getFunctions();
        metadata.getFunctionAndTypeManager().registerBuiltInFunctions(functions);
    }

    protected void registerFunctions(Plugin plugin)
    {
        functionAssertions.getMetadata().registerBuiltInFunctions(extractFunctions(plugin.getFunctions()));
    }

    protected void registerTypes(Plugin plugin)
    {
        for (Type type : plugin.getTypes()) {
            functionAssertions.getFunctionAndTypeManager().addType(type);
        }
    }

    protected static SqlDecimal decimal(String decimalString)
    {
        DecimalParseResult parseResult = Decimals.parseIncludeLeadingZerosInPrecision(decimalString);
        BigInteger unscaledValue;
        if (parseResult.getType().isShort()) {
            unscaledValue = BigInteger.valueOf((Long) parseResult.getObject());
        }
        else {
            unscaledValue = Decimals.decodeUnscaledValue((Slice) parseResult.getObject());
        }
        return new SqlDecimal(unscaledValue, parseResult.getType().getPrecision(), parseResult.getType().getScale());
    }

    protected static SqlDecimal maxPrecisionDecimal(long value)
    {
        final String maxPrecisionFormat = "%0" + (Decimals.MAX_PRECISION + (value < 0 ? 1 : 0)) + "d";
        return decimal(format(maxPrecisionFormat, value));
    }

    // this help function should only be used when the map contains null value
    // otherwise, use ImmutableMap.of()
    protected static Map asMap(List keyList, List valueList)
    {
        if (keyList.size() != valueList.size()) {
            fail("keyList should have same size with valueList");
        }
        Map map = new HashMap<>();
        for (int i = 0; i < keyList.size(); i++) {
            if (map.put(keyList.get(i), valueList.get(i)) != null) {
                fail("keyList should have same size with valueList");
            }
        }
        return map;
    }
}
