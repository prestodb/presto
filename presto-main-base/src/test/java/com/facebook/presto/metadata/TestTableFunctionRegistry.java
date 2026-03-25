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

import com.facebook.presto.Session;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.function.table.TableFunctionMetadata;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.connector.tvf.TestingTableFunctions.DuplicateArgumentsTableFunction;
import static com.facebook.presto.connector.tvf.TestingTableFunctions.MultipleRSTableFunction;
import static com.facebook.presto.connector.tvf.TestingTableFunctions.NullArgumentsTableFunction;
import static com.facebook.presto.connector.tvf.TestingTableFunctions.TestConnectorTableFunction;
import static com.facebook.presto.connector.tvf.TestingTableFunctions.TestConnectorTableFunction2;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.testing.assertions.Assert.assertNotNull;
import static com.facebook.presto.testing.assertions.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestTableFunctionRegistry
{
    private static final String CATALOG = "test_catalog";
    private static final String USER = "user";
    private static final String SCHEMA = "system";
    private static final Session SESSION = testSessionBuilder()
            .setCatalog(CATALOG)
            .setSchema(SCHEMA)
            .setIdentity(new Identity(USER, Optional.empty())).build();
    private static final Session MISMATCH_SESSION = testSessionBuilder()
            .setCatalog(CATALOG)
            .setSchema("other")
            .setIdentity(new Identity(USER, Optional.empty())).build();
    private static final String TEST_FUNCTION = "test_function";
    private static final String TEST_FUNCTION_2 = "test_function2";

    @Test
    public void testTableFunctionRegistry()
    {
        TableFunctionRegistry testFunctionRegistry = new TableFunctionRegistry();
        ConnectorId id = new ConnectorId(CATALOG);

        // Verify registration with multiple table functions.
        testFunctionRegistry.addTableFunctions(id, ImmutableList.of(new TestConnectorTableFunction(), new TestConnectorTableFunction2()));

        // Verify that table functions cannot be overridden for the same catalog.
        RuntimeException ex = expectThrows(IllegalStateException.class, () -> testFunctionRegistry.addTableFunctions(id, ImmutableList.of(new TestConnectorTableFunction())));
        assertTrue(ex.getMessage().contains("Table functions already registered for catalog: test"), ex.getMessage());

        // Verify table function resolution.
        assertNotNull(testFunctionRegistry.resolve(SESSION, QualifiedName.of(TEST_FUNCTION)));
        assertNotNull(testFunctionRegistry.resolve(SESSION, QualifiedName.of(TEST_FUNCTION_2)));
        assertNotNull(testFunctionRegistry.resolve(SESSION, QualifiedName.of("none")));
        assertNotNull(testFunctionRegistry.resolve(MISMATCH_SESSION, QualifiedName.of("none")));

        // Verify metadata.
        TableFunctionMetadata data = testFunctionRegistry.resolve(SESSION, QualifiedName.of(TEST_FUNCTION));
        assertEquals(data.getConnectorId(), id);
        assertTrue(data.getFunction() instanceof TestConnectorTableFunction);

        // Verify the removal of table functions.
        testFunctionRegistry.removeTableFunctions(id);
        assertNull(testFunctionRegistry.resolve(SESSION, QualifiedName.of(TEST_FUNCTION)));

        // Verify that null arguments table functions cannot be added.
        ex = expectThrows(NullPointerException.class, () -> testFunctionRegistry.addTableFunctions(id, ImmutableList.of(new NullArgumentsTableFunction())));
        assertTrue(ex.getMessage().contains("arguments is null"), ex.getMessage());

        // Verify that duplicate arguments table functions cannot be added.
        ex = expectThrows(IllegalArgumentException.class, () -> testFunctionRegistry.addTableFunctions(id, ImmutableList.of(new DuplicateArgumentsTableFunction())));
        assertTrue(ex.getMessage().contains("duplicate argument name: a"), ex.getMessage());

        // Verify that two row semantic table function arguments functions cannot be added.
        ex = expectThrows(IllegalArgumentException.class, () -> testFunctionRegistry.addTableFunctions(id, ImmutableList.of(new MultipleRSTableFunction())));
        assertTrue(ex.getMessage().contains("more than one table argument with row semantics"), ex.getMessage());
    }
}
