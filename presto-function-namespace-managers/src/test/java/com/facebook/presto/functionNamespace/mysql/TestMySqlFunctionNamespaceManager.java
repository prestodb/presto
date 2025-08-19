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
package com.facebook.presto.functionNamespace.mysql;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.drift.transport.netty.client.DriftNettyClientModule;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.functionNamespace.execution.SimpleAddressSqlFunctionExecutorsModule;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.testing.mysql.TestingMySqlServer;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.FUNCTION_POWER_TOWER_DOUBLE;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.FUNCTION_POWER_TOWER_DOUBLE_UPDATED;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.FUNCTION_POWER_TOWER_INT;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.FUNCTION_TANGENT;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.POWER_TOWER;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.TANGENT;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.TEST_CATALOG;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.TEST_SCHEMA;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.FunctionVersion.withVersion;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Comparator.comparing;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestMySqlFunctionNamespaceManager
{
    protected static final String DB = "presto";

    protected TestingMySqlServer mySqlServer;
    private Jdbi jdbi;
    private Injector injector;
    private MySqlFunctionNamespaceManager functionNamespaceManager;

    public Jdbi getJdbi()
    {
        return jdbi;
    }

    @BeforeClass
    public void setup()
            throws Exception
    {
        this.mySqlServer = new TestingMySqlServer("testuser", "testpass", DB);
        Bootstrap app = new Bootstrap(
                new MySqlFunctionNamespaceManagerModule(TEST_CATALOG),
                new SimpleAddressSqlFunctionExecutorsModule(),
                new DriftNettyClientModule(),
                new MySqlConnectionModule());

        try {
            this.injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(getConfig())
                    .initialize();
            this.functionNamespaceManager = injector.getInstance(MySqlFunctionNamespaceManager.class);
            this.jdbi = injector.getInstance(Jdbi.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    protected Map<String, String> getConfig()
    {
        return ImmutableMap.<String, String>builder()
                .put("function-cache-expiration", "0s")
                .put("function-instance-cache-expiration", "0s")
                .put("database-url", mySqlServer.getJdbcUrl(DB))
                .build();
    }

    @BeforeMethod
    public void setupFunctionNamespace()
    {
        createFunctionNamespace(TEST_CATALOG, TEST_SCHEMA);
    }

    @AfterMethod
    public void cleanup()
    {
        try (Handle handle = jdbi.open()) {
            handle.execute("DELETE FROM sql_functions");
            handle.execute("DELETE FROM function_namespaces");
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeQuietly(mySqlServer);
        if (injector != null) {
            injector.getInstance(LifeCycleManager.class).stop();
        }
    }

    @Test
    public void testListFunction()
    {
        createFunctionNamespace(TEST_CATALOG, "schema1");
        createFunctionNamespace(TEST_CATALOG, "schema2");
        SqlInvokedFunction function1 = constructTestFunction(QualifiedObjectName.valueOf(TEST_CATALOG, "schema1", "power_tower"));
        SqlInvokedFunction function2 = constructTestFunction(QualifiedObjectName.valueOf(TEST_CATALOG, "schema2", "power_tower"));

        createFunction(function1, false);
        createFunction(function2, false);
        assertListFunctions(function1.withVersion("1"), function2.withVersion("1"));
        assertListFunctions(Optional.of(format("%s.%%", TEST_CATALOG)), Optional.empty(), function1.withVersion("1"), function2.withVersion("1"));
        assertListFunctions(Optional.of(format("%s.%s.%%", TEST_CATALOG, "schema1")), Optional.empty(), function1.withVersion("1"));
        assertListFunctions(Optional.of("%schema%"), Optional.empty(), function1.withVersion("1"), function2.withVersion("1"));
        assertListFunctions(Optional.of("%power$_tower"), Optional.of("$"), function1.withVersion("1"), function2.withVersion("1"));
    }

    @Test
    public void testCreateFunction()
    {
        assertListFunctions();

        createFunction(FUNCTION_POWER_TOWER_DOUBLE, false);
        assertListFunctions(FUNCTION_POWER_TOWER_DOUBLE.withVersion("1"));

        createFunction(FUNCTION_POWER_TOWER_DOUBLE_UPDATED, true);
        assertListFunctions(FUNCTION_POWER_TOWER_DOUBLE_UPDATED.withVersion("2"));
        assertGetFunctions(POWER_TOWER, FUNCTION_POWER_TOWER_DOUBLE_UPDATED.withVersion("2"));

        createFunction(FUNCTION_POWER_TOWER_INT, true);
        assertListFunctions(FUNCTION_POWER_TOWER_DOUBLE_UPDATED.withVersion("2"), FUNCTION_POWER_TOWER_INT.withVersion("1"));
        assertGetFunctions(POWER_TOWER, FUNCTION_POWER_TOWER_DOUBLE_UPDATED.withVersion("2"), FUNCTION_POWER_TOWER_INT.withVersion("1"));

        createFunction(FUNCTION_TANGENT, true);
        assertListFunctions(FUNCTION_POWER_TOWER_DOUBLE_UPDATED.withVersion("2"), FUNCTION_POWER_TOWER_INT.withVersion("1"), FUNCTION_TANGENT.withVersion("1"));
        assertGetFunctions(POWER_TOWER, FUNCTION_POWER_TOWER_DOUBLE_UPDATED.withVersion("2"), FUNCTION_POWER_TOWER_INT.withVersion("1"));
        assertGetFunctions(TANGENT, FUNCTION_TANGENT.withVersion("1"));
    }

    @Test
    public void testCreateFunctionFailedDuplicate()
    {
        createFunction(FUNCTION_POWER_TOWER_DOUBLE, true);
        assertPrestoException(() -> createFunction(FUNCTION_POWER_TOWER_DOUBLE, false), ALREADY_EXISTS, ".*Function already exists: unittest\\.memory\\.power_tower\\(double\\)");
        assertPrestoException(() -> createFunction(FUNCTION_POWER_TOWER_DOUBLE_UPDATED, false), ALREADY_EXISTS, ".*Function already exists: unittest\\.memory\\.power_tower\\(double\\)");
    }

    public void testCreateFunctionRepeatedly()
    {
        createFunction(FUNCTION_POWER_TOWER_DOUBLE, false);
        assertListFunctions(FUNCTION_POWER_TOWER_DOUBLE.withVersion("1"));

        createFunction(FUNCTION_POWER_TOWER_DOUBLE, true);
        assertListFunctions(FUNCTION_POWER_TOWER_DOUBLE.withVersion("1"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*function 'unittest\\.memory\\.power_tower\\(x double\\):double:1 \\[SCALAR] \\{RETURN pow\\(x, x\\)\\} \\(SQL, DETERMINISTIC, CALLED_ON_NULL_INPUT\\)' is already versioned")
    public void testCreateFunctionFailedVersioned()
    {
        createFunction(FUNCTION_POWER_TOWER_DOUBLE.withVersion("1"), true);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Schema name exceeds max length of 128.*")
    public void testSchemaNameTooLong()
    {
        QualifiedObjectName functionName = QualifiedObjectName.valueOf(TEST_CATALOG, dummyString(129), "tangent");
        createFunction(createFunctionTangent(functionName), false);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Function name exceeds max length of 256.*")
    public void testFunctionNameTooLong()
    {
        QualifiedObjectName functionName = QualifiedObjectName.valueOf(TEST_CATALOG, TEST_SCHEMA, dummyString(257));
        createFunction(createFunctionTangent(functionName), false);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Function has more than 100 parameters: 101")
    public void testTooManyParameters()
    {
        List<Parameter> parameters = nCopies(101, new Parameter("x", parseTypeSignature(DOUBLE)));
        createFunction(createFunctionTangent(parameters), false);
    }

    public void testLargeParameterCount()
    {
        List<Parameter> parameters = nCopies(100, new Parameter("x", parseTypeSignature(DOUBLE)));
        createFunction(createFunctionTangent(parameters), false);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Parameter name exceeds max length of 100.*")
    public void testParameterNameTooLong()
    {
        List<Parameter> parameters = ImmutableList.of(new Parameter(dummyString(101), parseTypeSignature(DOUBLE)));
        createFunction(createFunctionTangent(parameters), false);
    }

    public void testLongParameterName()
    {
        List<Parameter> parameters = ImmutableList.of(new Parameter(dummyString(100), parseTypeSignature(DOUBLE)));
        createFunction(createFunctionTangent(parameters), false);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Parameter type list exceeds max length of 30000.*")
    public void testParameterTypeListTooLong()
    {
        List<Parameter> parameters = ImmutableList.of(new Parameter("x", createLargeRowType(4286)));
        createFunction(createFunctionTangent(parameters), false);
    }

    public void testLongParameterTypeList()
    {
        List<Parameter> parameters = ImmutableList.of(new Parameter("x", createLargeRowType(4285)));
        createFunction(createFunctionTangent(parameters), false);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Return type exceeds max length of 30000.*")
    public void testReturnTypeTooLong()
    {
        TypeSignature returnType = parseTypeSignature(dummyString(30001));
        createFunction(createFunctionTangent(returnType), false);
    }

    public void testLongReturnType()
    {
        TypeSignature returnType = parseTypeSignature(dummyString(30000));
        createFunction(createFunctionTangent(returnType), false);
    }

    @Test
    public void testAlterFunction()
    {
        createFunction(FUNCTION_POWER_TOWER_DOUBLE, false);
        createFunction(FUNCTION_POWER_TOWER_INT, false);
        createFunction(FUNCTION_TANGENT, false);

        // Alter a specific function by name and parameter types
        alterFunction(POWER_TOWER, Optional.of(ImmutableList.of(parseTypeSignature(DOUBLE))), new AlterRoutineCharacteristics(Optional.of(RETURNS_NULL_ON_NULL_INPUT)));
        assertGetFunctions(
                POWER_TOWER,
                FUNCTION_POWER_TOWER_INT.withVersion("1"),
                new SqlInvokedFunction(
                        POWER_TOWER,
                        ImmutableList.of(new Parameter("x", parseTypeSignature(DOUBLE))),
                        parseTypeSignature(DOUBLE),
                        "power tower",
                        RoutineCharacteristics.builder().setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
                        "RETURN pow(x, x)",
                        withVersion("2")));

        // Drop function and alter function by name
        dropFunction(POWER_TOWER, Optional.of(ImmutableList.of(parseTypeSignature(DOUBLE))), false);
        alterFunction(POWER_TOWER, Optional.empty(), new AlterRoutineCharacteristics(Optional.of(CALLED_ON_NULL_INPUT)));

        // Alter function by name
        alterFunction(TANGENT, Optional.empty(), new AlterRoutineCharacteristics(Optional.of(CALLED_ON_NULL_INPUT)));
        SqlInvokedFunction tangentV2 = new SqlInvokedFunction(
                TANGENT,
                FUNCTION_TANGENT.getParameters(),
                FUNCTION_TANGENT.getSignature().getReturnType(),
                FUNCTION_TANGENT.getDescription(),
                RoutineCharacteristics.builder().setDeterminism(DETERMINISTIC).build(),
                FUNCTION_TANGENT.getBody(),
                withVersion("2"));
        assertGetFunctions(TANGENT, tangentV2);

        // Alter function with no change
        alterFunction(TANGENT, Optional.empty(), new AlterRoutineCharacteristics(Optional.of(CALLED_ON_NULL_INPUT)));
        assertGetFunctions(TANGENT, tangentV2);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Function 'unittest\\.memory\\.power_tower' has multiple signatures: unittest\\.memory\\.power_tower(\\(integer\\):integer|\\(double\\):double); unittest\\.memory\\.power_tower(\\(double\\):double|\\(integer\\):integer)\\. Please specify parameter types\\.")
    public void testAlterFunctionAmbiguous()
    {
        createFunction(FUNCTION_POWER_TOWER_DOUBLE, false);
        createFunction(FUNCTION_POWER_TOWER_INT, false);
        alterFunction(POWER_TOWER, Optional.empty(), new AlterRoutineCharacteristics(Optional.of(RETURNS_NULL_ON_NULL_INPUT)));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Function not found: unittest\\.memory\\.power_tower\\(\\)")
    public void testAlterFunctionNotFound()
    {
        createFunction(FUNCTION_POWER_TOWER_DOUBLE, false);
        createFunction(FUNCTION_POWER_TOWER_INT, false);
        alterFunction(POWER_TOWER, Optional.of(ImmutableList.of()), new AlterRoutineCharacteristics(Optional.of(RETURNS_NULL_ON_NULL_INPUT)));
    }

    @Test
    public void testDropFunction()
    {
        // Create functions
        createFunction(FUNCTION_POWER_TOWER_DOUBLE, false);
        createFunction(FUNCTION_POWER_TOWER_INT, false);
        createFunction(FUNCTION_TANGENT, false);

        // Drop function by name
        dropFunction(TANGENT, Optional.empty(), false);
        assertGetFunctions(TANGENT);

        // Recreate functions
        createFunction(FUNCTION_TANGENT, false);

        // Drop a specific function by name and parameter types
        dropFunction(POWER_TOWER, Optional.of(ImmutableList.of(parseTypeSignature(DOUBLE))), true);
        assertGetFunctions(POWER_TOWER, FUNCTION_POWER_TOWER_INT.withVersion("1"));

        dropFunction(TANGENT, Optional.of(ImmutableList.of(parseTypeSignature(DOUBLE))), true);
        assertGetFunctions(POWER_TOWER, FUNCTION_POWER_TOWER_INT.withVersion("1"));
        assertGetFunctions(TANGENT);

        // Recreate functions, power_double(double) is created with a different definition
        createFunction(FUNCTION_POWER_TOWER_DOUBLE_UPDATED, false);
        createFunction(FUNCTION_TANGENT, false);

        // Drop functions consecutively
        dropFunction(POWER_TOWER, Optional.of(ImmutableList.of(parseTypeSignature(DOUBLE))), false);
        dropFunction(POWER_TOWER, Optional.empty(), false);
    }

    public void testDropFunctionMultiple()
    {
        createFunction(FUNCTION_POWER_TOWER_DOUBLE, false);
        createFunction(FUNCTION_POWER_TOWER_INT, false);
        dropFunction(POWER_TOWER, Optional.empty(), false);
        assertGetFunctions(POWER_TOWER);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Function not found: unittest\\.memory\\.power_tower\\(\\)")
    public void testDropFunctionNotFound()
    {
        createFunction(FUNCTION_POWER_TOWER_DOUBLE, false);
        createFunction(FUNCTION_POWER_TOWER_INT, false);
        dropFunction(POWER_TOWER, Optional.of(ImmutableList.of()), true);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Function not found: unittest\\.memory\\.tangent\\(double\\)")
    public void testDropFunctionFailed()
    {
        dropFunction(TANGENT, Optional.of(ImmutableList.of(parseTypeSignature(DOUBLE))), false);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Function not found: unittest\\.memory\\.invalid")
    public void testDropFunctionsFailed()
    {
        dropFunction(QualifiedObjectName.valueOf(TEST_CATALOG, TEST_SCHEMA, "invalid"), Optional.empty(), false);
    }

    @Test
    public void testGetFunctionMetadata()
    {
        createFunction(FUNCTION_POWER_TOWER_DOUBLE, true);
        FunctionHandle handle1 = getLatestFunctionHandle(FUNCTION_POWER_TOWER_DOUBLE.getFunctionId());
        assertGetFunctionMetadata(handle1, FUNCTION_POWER_TOWER_DOUBLE);

        createFunction(FUNCTION_POWER_TOWER_DOUBLE_UPDATED, true);
        FunctionHandle handle2 = getLatestFunctionHandle(FUNCTION_POWER_TOWER_DOUBLE_UPDATED.getFunctionId());
        assertGetFunctionMetadata(handle1, FUNCTION_POWER_TOWER_DOUBLE);
        assertGetFunctionMetadata(handle2, FUNCTION_POWER_TOWER_DOUBLE_UPDATED);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Error getting FunctionMetadata for handle: unittest\\.memory\\.power_tower\\(double\\):2")
    public void testInvalidFunctionHandle()
    {
        createFunction(FUNCTION_POWER_TOWER_DOUBLE, true);
        SqlFunctionHandle functionHandle = new SqlFunctionHandle(FUNCTION_POWER_TOWER_DOUBLE.getFunctionId(), "2");
        functionNamespaceManager.getFunctionMetadata(functionHandle);
    }

    private void createFunctionNamespace(String catalog, String schema)
    {
        try (Handle handle = jdbi.open()) {
            handle.execute("INSERT INTO function_namespaces VALUES (?, ?)", catalog, schema);
        }
    }

    private void createFunction(SqlInvokedFunction function, boolean replace)
    {
        functionNamespaceManager.createFunction(function, replace);
    }

    private void alterFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics characteristics)
    {
        functionNamespaceManager.alterFunction(functionName, parameterTypes, characteristics);
    }

    private void dropFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
    {
        functionNamespaceManager.dropFunction(functionName, parameterTypes, exists);
    }

    private FunctionHandle getLatestFunctionHandle(SqlFunctionId functionId)
    {
        FunctionNamespaceTransactionHandle transactionHandle = functionNamespaceManager.beginTransaction();
        Optional<SqlInvokedFunction> function = functionNamespaceManager.getFunctions(Optional.of(transactionHandle), functionId.getFunctionName()).stream()
                .filter(candidate -> candidate.getFunctionId().equals(functionId))
                .max(comparing(SqlInvokedFunction::getRequiredVersion));
        assertTrue(function.isPresent());
        functionNamespaceManager.commit(transactionHandle);
        return function.orElseThrow().getRequiredFunctionHandle();
    }

    private void assertListFunctions(Optional<String> likePattern, Optional<String> escape, SqlInvokedFunction... functions)
    {
        assertEquals(ImmutableSet.copyOf(functionNamespaceManager.listFunctions(likePattern, escape)), ImmutableSet.copyOf(Arrays.asList(functions)));
    }

    private void assertListFunctions(SqlInvokedFunction... functions)
    {
        assertListFunctions(Optional.empty(), Optional.empty(), functions);
    }

    private void assertGetFunctions(QualifiedObjectName functionName, SqlInvokedFunction... functions)
    {
        FunctionNamespaceTransactionHandle transactionHandle = functionNamespaceManager.beginTransaction();
        assertEquals(ImmutableSet.copyOf(functionNamespaceManager.getFunctions(Optional.of(transactionHandle), functionName)), ImmutableSet.copyOf(Arrays.asList(functions)));
        functionNamespaceManager.commit(transactionHandle);
    }

    private void assertGetFunctionMetadata(FunctionHandle functionHandle, SqlInvokedFunction expectFunction)
    {
        FunctionMetadata functionMetadata = functionNamespaceManager.getFunctionMetadata(functionHandle);

        assertEquals(functionMetadata.getName(), expectFunction.getSignature().getName());
        assertFalse(functionMetadata.getOperatorType().isPresent());
        assertEquals(functionMetadata.getArgumentTypes(), expectFunction.getSignature().getArgumentTypes());
        assertEquals(functionMetadata.getReturnType(), expectFunction.getSignature().getReturnType());
        assertEquals(functionMetadata.getFunctionKind(), SCALAR);
        assertEquals(functionMetadata.isDeterministic(), expectFunction.isDeterministic());
        assertEquals(functionMetadata.isCalledOnNullInput(), expectFunction.isCalledOnNullInput());
    }

    private static void assertPrestoException(Runnable runnable, ErrorCodeSupplier errorCode, String expectedMessageRegex)
    {
        try {
            runnable.run();
            fail("No Exception is thrown, expect PrestoException");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), errorCode.toErrorCode());
            assertTrue(e.getMessage().matches(expectedMessageRegex), format("Error message '%s' does not match '%s'", e.getMessage(), expectedMessageRegex));
        }
    }

    private static String dummyString(int length)
    {
        return Joiner.on("").join(nCopies(length, "x"));
    }

    private static SqlInvokedFunction constructTestFunction(QualifiedObjectName functionName)
    {
        return new SqlInvokedFunction(
                functionName,
                ImmutableList.of(new Parameter("x", parseTypeSignature(DOUBLE))),
                parseTypeSignature(DOUBLE),
                "power tower",
                RoutineCharacteristics.builder().setDeterminism(DETERMINISTIC).build(),
                "pow(x, x)",
                notVersioned());
    }

    private static SqlInvokedFunction createFunctionTangent(QualifiedObjectName functionName)
    {
        return new SqlInvokedFunction(
                functionName,
                FUNCTION_TANGENT.getParameters(),
                FUNCTION_TANGENT.getSignature().getReturnType(),
                FUNCTION_TANGENT.getDescription(),
                FUNCTION_TANGENT.getRoutineCharacteristics(),
                FUNCTION_TANGENT.getBody(),
                notVersioned());
    }

    private static SqlInvokedFunction createFunctionTangent(List<Parameter> parameters)
    {
        return new SqlInvokedFunction(
                FUNCTION_TANGENT.getFunctionId().getFunctionName(),
                parameters,
                FUNCTION_TANGENT.getSignature().getReturnType(),
                FUNCTION_TANGENT.getDescription(),
                FUNCTION_TANGENT.getRoutineCharacteristics(),
                FUNCTION_TANGENT.getBody(),
                notVersioned());
    }

    private static SqlInvokedFunction createFunctionTangent(TypeSignature returnType)
    {
        return new SqlInvokedFunction(
                TANGENT,
                FUNCTION_TANGENT.getParameters(),
                returnType,
                FUNCTION_TANGENT.getDescription(),
                FUNCTION_TANGENT.getRoutineCharacteristics(),
                FUNCTION_TANGENT.getBody(),
                notVersioned());
    }

    private static TypeSignature createLargeRowType(int fieldCount)
    {
        String format = format("ROW(%s)", Joiner.on(",").join(nCopies(fieldCount, DOUBLE)));
        return parseTypeSignature(format);
    }
}
