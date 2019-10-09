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
import com.facebook.presto.testing.InMemoryFunctionNamespaceManager;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.SqlInvokedRegularFunctionTestUtils.FUNCTION_POWER_TOWER_DOUBLE;
import static com.facebook.presto.metadata.SqlInvokedRegularFunctionTestUtils.FUNCTION_POWER_TOWER_DOUBLE_UPDATED;
import static com.facebook.presto.metadata.SqlInvokedRegularFunctionTestUtils.FUNCTION_POWER_TOWER_INT;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.sqlfunction.SqlInvokedRegularFunction.versioned;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
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

    private static InMemoryFunctionNamespaceManager createFunctionNamespaceManager()
    {
        return new InMemoryFunctionNamespaceManager();
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
