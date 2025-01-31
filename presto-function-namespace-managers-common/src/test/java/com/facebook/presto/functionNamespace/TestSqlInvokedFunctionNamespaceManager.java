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
package com.facebook.presto.functionNamespace;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutor;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.functionNamespace.testing.InMemoryFunctionNamespaceManager;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.FUNCTION_POWER_TOWER_DOUBLE;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.FUNCTION_POWER_TOWER_DOUBLE_UPDATED;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.FUNCTION_POWER_TOWER_INT;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.POWER_TOWER;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.TEST_CATALOG;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.SQL;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSqlInvokedFunctionNamespaceManager
{
    @Test
    public void testCreateFunction()
    {
        InMemoryFunctionNamespaceManager functionNamespaceManager = createFunctionNamespaceManager();
        functionNamespaceManager.createFunction(FUNCTION_POWER_TOWER_DOUBLE, false);
        assertEquals(functionNamespaceManager.listFunctions(Optional.empty(), Optional.empty()), ImmutableSet.of(FUNCTION_POWER_TOWER_DOUBLE.withVersion("1")));

        functionNamespaceManager.createFunction(FUNCTION_POWER_TOWER_INT, false);
        assertEquals(
                ImmutableSet.copyOf(functionNamespaceManager.listFunctions(Optional.empty(), Optional.empty())),
                ImmutableSet.of(FUNCTION_POWER_TOWER_DOUBLE.withVersion("1"), FUNCTION_POWER_TOWER_INT.withVersion("1")));

        functionNamespaceManager.createFunction(FUNCTION_POWER_TOWER_DOUBLE_UPDATED, true);
        assertEquals(
                ImmutableSet.copyOf(functionNamespaceManager.listFunctions(Optional.empty(), Optional.empty())),
                ImmutableSet.of(FUNCTION_POWER_TOWER_DOUBLE_UPDATED.withVersion("2"), FUNCTION_POWER_TOWER_INT.withVersion("1")));
    }

    @Test
    public void testCreateFunctionFailed()
    {
        InMemoryFunctionNamespaceManager functionNamespaceManager = createFunctionNamespaceManager();
        functionNamespaceManager.createFunction(FUNCTION_POWER_TOWER_DOUBLE, false);
        assertPrestoException(
                () -> functionNamespaceManager.createFunction(FUNCTION_POWER_TOWER_DOUBLE_UPDATED, false),
                GENERIC_USER_ERROR,
                ".*Function 'unittest.memory.power_tower\\(double\\)' already exists");
    }

    @Test
    public void testTransactionalGetFunction()
    {
        InMemoryFunctionNamespaceManager functionNamespaceManager = new InMemoryFunctionNamespaceManager(
                TEST_CATALOG,
                new SqlFunctionExecutors(
                        ImmutableMap.of(SQL, FunctionImplementationType.SQL),
                        new NoopSqlFunctionExecutor()),
                new SqlInvokedFunctionNamespaceManagerConfig()
                        .setFunctionCacheExpiration(new Duration(0, MILLISECONDS))
                        .setFunctionInstanceCacheExpiration(new Duration(0, MILLISECONDS)));

        // begin first transaction
        FunctionNamespaceTransactionHandle transaction1 = functionNamespaceManager.beginTransaction();
        assertEquals(functionNamespaceManager.getFunctions(Optional.of(transaction1), POWER_TOWER).size(), 0);

        // create function, first transaction still sees no functions
        functionNamespaceManager.createFunction(FUNCTION_POWER_TOWER_DOUBLE, false);
        assertEquals(functionNamespaceManager.getFunctions(Optional.of(transaction1), POWER_TOWER).size(), 0);

        // second transaction sees newly created function
        FunctionNamespaceTransactionHandle transaction2 = functionNamespaceManager.beginTransaction();
        Collection<SqlInvokedFunction> functions2 = functionNamespaceManager.getFunctions(Optional.of(transaction2), POWER_TOWER);
        assertEquals(functions2.size(), 1);
        assertEquals(getOnlyElement(functions2), FUNCTION_POWER_TOWER_DOUBLE.withVersion("1"));

        // update the function, second transaction still sees the old functions
        functionNamespaceManager.createFunction(FUNCTION_POWER_TOWER_DOUBLE_UPDATED, true);
        functions2 = functionNamespaceManager.getFunctions(Optional.of(transaction2), POWER_TOWER);
        assertEquals(functions2.size(), 1);
        assertEquals(getOnlyElement(functions2), FUNCTION_POWER_TOWER_DOUBLE.withVersion("1"));

        // third transaction sees the updated function
        FunctionNamespaceTransactionHandle transaction3 = functionNamespaceManager.beginTransaction();
        Collection<SqlInvokedFunction> functions3 = functionNamespaceManager.getFunctions(Optional.of(transaction3), POWER_TOWER);
        assertEquals(functions3.size(), 1);
        assertEquals(getOnlyElement(functions3), FUNCTION_POWER_TOWER_DOUBLE_UPDATED.withVersion("2"));

        functionNamespaceManager.commit(transaction1);
        functionNamespaceManager.commit(transaction2);
        functionNamespaceManager.commit(transaction3);
    }

    @Test
    public void testCaching()
    {
        InMemoryFunctionNamespaceManager functionNamespaceManager = createFunctionNamespaceManager();
        functionNamespaceManager.createFunction(FUNCTION_POWER_TOWER_DOUBLE, false);

        // fetchFunctionsDirect does not produce the same function reference
        SqlInvokedFunction function1 = getOnlyElement(functionNamespaceManager.fetchFunctionsDirect(POWER_TOWER));
        SqlInvokedFunction function2 = getOnlyElement(functionNamespaceManager.fetchFunctionsDirect(POWER_TOWER));
        assertEquals(function1, function2);
        assertNotSame(function1, function2);

        // fetchFunctionMetadataDirect does not produce the same metadata reference
        FunctionMetadata metadata1 = functionNamespaceManager.fetchFunctionMetadataDirect(function1.getRequiredFunctionHandle());
        FunctionMetadata metadata2 = functionNamespaceManager.fetchFunctionMetadataDirect(function2.getRequiredFunctionHandle());
        assertEquals(metadata1, metadata2);
        assertNotSame(metadata1, metadata2);

        // getFunctionMetadata produces the same metadata reference
        metadata1 = functionNamespaceManager.getFunctionMetadata(function1.getRequiredFunctionHandle());
        metadata2 = functionNamespaceManager.getFunctionMetadata(function2.getRequiredFunctionHandle());
        assertSame(metadata1, metadata2);

        // getFunctions produces the same function collection reference
        functionNamespaceManager.createFunction(FUNCTION_POWER_TOWER_INT, false);
        FunctionNamespaceTransactionHandle transaction1 = functionNamespaceManager.beginTransaction();
        FunctionNamespaceTransactionHandle transaction2 = functionNamespaceManager.beginTransaction();
        Collection<SqlInvokedFunction> functions1 = functionNamespaceManager.getFunctions(Optional.of(transaction1), POWER_TOWER);
        Collection<SqlInvokedFunction> functions2 = functionNamespaceManager.getFunctions(Optional.of(transaction2), POWER_TOWER);
        assertEquals(functions1.size(), 2);
        assertSame(functions1, functions2);
    }

    @Test
    public void testErrorHandling()
    {
        FunctionNamespaceManager functionNamespaceManager = new ErrorThrowingFunctionNamespaceManager(
                TEST_CATALOG,
                new SqlFunctionExecutors(ImmutableMap.of(), new NoopSqlFunctionExecutor()),
                new SqlInvokedFunctionNamespaceManagerConfig());

        // ErrorThrowingFunctionNamespaceManager returns a NOT_FOUND error fetching the type, so this function should return Optional.empty()
        assertEquals(functionNamespaceManager.getUserDefinedType(QualifiedObjectName.valueOf("catalog.schema.type")), Optional.empty());

        // ErrorThrowingFunctionNamespaceManager throws a PrestoException that gets propagated
        assertPrestoException(() -> functionNamespaceManager.getFunctionMetadata(new SqlFunctionHandle(FUNCTION_POWER_TOWER_DOUBLE.getFunctionId(), "123")), GENERIC_USER_ERROR, "Error fetching function metadata");
        assertPrestoException(() -> functionNamespaceManager.getFunctionHandle(Optional.empty(), FUNCTION_POWER_TOWER_DOUBLE.getSignature()), GENERIC_USER_ERROR, "Error fetching functions");

        // ErrorThrowingFunctionNamespaceManager throws an exception that is not a PrestoException. It gets wrapped in a PrestoException GENERIC_INTERNAL_ERROR
        assertPrestoException(() -> functionNamespaceManager.getScalarFunctionImplementation(new SqlFunctionHandle(FUNCTION_POWER_TOWER_DOUBLE.getFunctionId(), "123")), GENERIC_INTERNAL_ERROR, "Error getting ScalarFunctionImplementation for handle: unittest\\.memory\\.power_tower\\(double\\):123");
    }

    private static InMemoryFunctionNamespaceManager createFunctionNamespaceManager()
    {
        return new InMemoryFunctionNamespaceManager(
                TEST_CATALOG,
                new SqlFunctionExecutors(
                        ImmutableMap.of(SQL, FunctionImplementationType.SQL),
                        new NoopSqlFunctionExecutor()),
                new SqlInvokedFunctionNamespaceManagerConfig());
    }

    private static void assertPrestoException(Runnable runnable, ErrorCodeSupplier expectedErrorCode, String expectedMessageRegex)
    {
        try {
            runnable.run();
            fail(format("Expected PrestoException with error code '%s', but not Exception is thrown", expectedErrorCode.toErrorCode().getName()));
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), expectedErrorCode.toErrorCode());
            assertTrue(e.getMessage().matches(expectedMessageRegex), format("Messages did not match. Actual message: \"%s\", Expected regex: \"%s\"", e.getMessage(), expectedMessageRegex));
        }
    }

    private class ErrorThrowingFunctionNamespaceManager
            extends AbstractSqlInvokedFunctionNamespaceManager
    {
        public ErrorThrowingFunctionNamespaceManager(String catalogName, SqlFunctionExecutors sqlFunctionExecutors, SqlInvokedFunctionNamespaceManagerConfig config)
        {
            super(catalogName, sqlFunctionExecutors, config);
        }

        @Override
        protected Collection<SqlInvokedFunction> fetchFunctionsDirect(QualifiedObjectName functionName)
        {
            throw new PrestoException(GENERIC_USER_ERROR, "Error fetching functions");
        }

        @Override
        protected UserDefinedType fetchUserDefinedTypeDirect(QualifiedObjectName typeName)
        {
            throw new PrestoException(NOT_FOUND, "type not found");
        }

        @Override
        protected FunctionMetadata fetchFunctionMetadataDirect(SqlFunctionHandle functionHandle)
        {
            throw new PrestoException(GENERIC_USER_ERROR, "Error fetching function metadata");
        }

        @Override
        protected ScalarFunctionImplementation fetchFunctionImplementationDirect(SqlFunctionHandle functionHandle)
        {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void createFunction(SqlInvokedFunction function, boolean replace)
        {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void alterFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics)
        {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void dropFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
        {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public Collection<SqlInvokedFunction> listFunctions(Optional<String> likePattern, Optional<String> escape)
        {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void addUserDefinedType(UserDefinedType userDefinedType)
        {
            throw new UnsupportedOperationException("Not implemented");
        }
    }
}
