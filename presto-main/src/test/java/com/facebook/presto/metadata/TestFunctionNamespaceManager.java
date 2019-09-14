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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.sqlfunction.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.sqlfunction.SqlInvokedRegularFunction;
import com.facebook.presto.testing.InMemoryFunctionNamespaceManager;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Optional;

import static com.facebook.presto.metadata.SqlInvokedRegularFunctionTestUtils.FUNCTION_POWER_TOWER_DOUBLE;
import static com.facebook.presto.metadata.SqlInvokedRegularFunctionTestUtils.FUNCTION_POWER_TOWER_DOUBLE_UPDATED;
import static com.facebook.presto.metadata.SqlInvokedRegularFunctionTestUtils.FUNCTION_POWER_TOWER_INT;
import static com.facebook.presto.metadata.SqlInvokedRegularFunctionTestUtils.POWER_TOWER;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.sqlfunction.SqlInvokedRegularFunction.versioned;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestFunctionNamespaceManager
{
    @Test
    public void testCreateFunction()
    {
        InMemoryFunctionNamespaceManager functionNamespaceManager = createFunctionNamespaceManager();
        functionNamespaceManager.createFunction(FUNCTION_POWER_TOWER_DOUBLE, false);
        assertEquals(functionNamespaceManager.listFunctions(), ImmutableSet.of(versioned(FUNCTION_POWER_TOWER_DOUBLE, 1)));

        functionNamespaceManager.createFunction(FUNCTION_POWER_TOWER_INT, false);
        assertEquals(
                ImmutableSet.copyOf(functionNamespaceManager.listFunctions()),
                ImmutableSet.of(versioned(FUNCTION_POWER_TOWER_DOUBLE, 1), versioned(FUNCTION_POWER_TOWER_INT, 1)));

        functionNamespaceManager.createFunction(FUNCTION_POWER_TOWER_DOUBLE_UPDATED, true);
        assertEquals(
                ImmutableSet.copyOf(functionNamespaceManager.listFunctions()),
                ImmutableSet.of(versioned(FUNCTION_POWER_TOWER_DOUBLE_UPDATED, 2), versioned(FUNCTION_POWER_TOWER_INT, 1)));

        System.out.println(FUNCTION_POWER_TOWER_DOUBLE);
    }

    @Test
    public void testCreateFunctionFailed()
    {
        InMemoryFunctionNamespaceManager functionNamespaceManager = createFunctionNamespaceManager();
        functionNamespaceManager.createFunction(FUNCTION_POWER_TOWER_DOUBLE, false);
        assertPrestoException(
                () -> functionNamespaceManager.createFunction(FUNCTION_POWER_TOWER_DOUBLE_UPDATED, false),
                GENERIC_USER_ERROR,
                ".*Function 'unittest.memory.power_tower' already exists");
    }

    @Test
    public void testTransactionalGetFunction()
    {
        InMemoryFunctionNamespaceManager functionNamespaceManager = new InMemoryFunctionNamespaceManager(
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
        Collection<SqlInvokedRegularFunction> functions2 = functionNamespaceManager.getFunctions(Optional.of(transaction2), POWER_TOWER);
        assertEquals(functions2.size(), 1);
        assertEquals(getOnlyElement(functions2), versioned(FUNCTION_POWER_TOWER_DOUBLE, 1));

        // update the function, second transaction still sees the old functions
        functionNamespaceManager.createFunction(FUNCTION_POWER_TOWER_DOUBLE_UPDATED, true);
        functions2 = functionNamespaceManager.getFunctions(Optional.of(transaction2), POWER_TOWER);
        assertEquals(functions2.size(), 1);
        assertEquals(getOnlyElement(functions2), versioned(FUNCTION_POWER_TOWER_DOUBLE, 1));

        // third transaction sees the updated function
        FunctionNamespaceTransactionHandle transaction3 = functionNamespaceManager.beginTransaction();
        Collection<SqlInvokedRegularFunction> functions3 = functionNamespaceManager.getFunctions(Optional.of(transaction3), POWER_TOWER);
        assertEquals(functions3.size(), 1);
        assertEquals(getOnlyElement(functions3), versioned(FUNCTION_POWER_TOWER_DOUBLE_UPDATED, 2));

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
        SqlInvokedRegularFunction function1 = getOnlyElement(functionNamespaceManager.fetchFunctionsDirect(POWER_TOWER));
        SqlInvokedRegularFunction function2 = getOnlyElement(functionNamespaceManager.fetchFunctionsDirect(POWER_TOWER));
        assertEquals(function1, function2);
        assertNotSame(function1, function2);

        // fetchFunctionMetadataDirect does not produce the same metdata reference
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
        Collection<SqlInvokedRegularFunction> functions1 = functionNamespaceManager.getFunctions(Optional.of(transaction1), POWER_TOWER);
        Collection<SqlInvokedRegularFunction> functions2 = functionNamespaceManager.getFunctions(Optional.of(transaction2), POWER_TOWER);
        assertEquals(functions1.size(), 2);
        assertSame(functions1, functions2);
    }

    private static InMemoryFunctionNamespaceManager createFunctionNamespaceManager()
    {
        return new InMemoryFunctionNamespaceManager(new SqlInvokedFunctionNamespaceManagerConfig());
    }

    private static void assertPrestoException(Runnable runnable, ErrorCodeSupplier expectedErrorCode, String expectedMessageRegex)
    {
        try {
            runnable.run();
            fail(format("Expected PrestoException with error code '%s', but not Exception is thrown", expectedErrorCode.toErrorCode().getName()));
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), expectedErrorCode.toErrorCode());
            assertTrue(e.getMessage().matches(expectedMessageRegex));
        }
    }
}
