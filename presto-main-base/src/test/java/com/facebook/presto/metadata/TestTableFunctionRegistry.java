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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.ArgumentSpecification;
import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.facebook.presto.spi.function.table.ReturnTypeSpecification;
import com.facebook.presto.spi.function.table.ScalarArgumentSpecification;
import com.facebook.presto.spi.function.table.TableArgumentSpecification;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestTableFunctionRegistry
{
    private static final String CATALOG = "test_catalog";
    private static final String USER = "user";
    private static final String SCHEMA = "test_schema";
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
        assertTrue(testFunctionRegistry.resolve(SESSION, QualifiedName.of(TEST_FUNCTION)).isPresent());
        assertTrue(testFunctionRegistry.resolve(SESSION, QualifiedName.of(TEST_FUNCTION_2)).isPresent());
        assertFalse(testFunctionRegistry.resolve(SESSION, QualifiedName.of("none")).isPresent());
        assertFalse(testFunctionRegistry.resolve(MISMATCH_SESSION, QualifiedName.of("none")).isPresent());

        // Verify metadata.
        TableFunctionMetadata data = testFunctionRegistry.resolve(SESSION, QualifiedName.of(TEST_FUNCTION)).get();
        assertEquals(data.getConnectorId(), id);
        assertTrue(data.getFunction() instanceof TestConnectorTableFunction);

        // Verify the removal of table functions.
        testFunctionRegistry.removeTableFunctions(id);
        assertFalse(testFunctionRegistry.resolve(SESSION, QualifiedName.of(TEST_FUNCTION)).isPresent());

        // Verify that null arguments table functions cannot be added.
        ex = expectThrows(NullPointerException.class, () -> testFunctionRegistry.addTableFunctions(id, ImmutableList.of(new NullArgumentsTableFunction())));
        assertTrue(ex.getMessage().contains("table function arguments is null"), ex.getMessage());

        // Verify that duplicate arguments table functions cannot be added.
        ex = expectThrows(IllegalArgumentException.class, () -> testFunctionRegistry.addTableFunctions(id, ImmutableList.of(new DuplicateArgumentsTableFunction())));
        assertTrue(ex.getMessage().contains("duplicate argument name: a"), ex.getMessage());

        // Verify that two row semantic table function arguments functions cannot be added.
        ex = expectThrows(IllegalArgumentException.class, () -> testFunctionRegistry.addTableFunctions(id, ImmutableList.of(new MultipleRTSTableFunction())));
        assertTrue(ex.getMessage().contains("more than one table argument with row semantics"), ex.getMessage());
    }

    private static class TestConnectorTableFunction
            implements ConnectorTableFunction
    {
        @Override
        public String getSchema()
        {
            return SCHEMA;
        }

        @Override
        public String getName()
        {
            return TEST_FUNCTION;
        }

        @Override
        public List<ArgumentSpecification> getArguments()
        {
            return ImmutableList.of();
        }

        @Override
        public ReturnTypeSpecification getReturnTypeSpecification()
        {
            return ReturnTypeSpecification.OnlyPassThrough.ONLY_PASS_THROUGH;
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return null;
        }
    }

    private static class TestConnectorTableFunction2
            implements ConnectorTableFunction
    {
        @Override
        public String getSchema()
        {
            return SCHEMA;
        }

        @Override
        public String getName()
        {
            return TEST_FUNCTION_2;
        }

        @Override
        public List<ArgumentSpecification> getArguments()
        {
            return ImmutableList.of();
        }

        @Override
        public ReturnTypeSpecification getReturnTypeSpecification()
        {
            return ReturnTypeSpecification.OnlyPassThrough.ONLY_PASS_THROUGH;
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return null;
        }
    }

    private static class NullArgumentsTableFunction
            implements ConnectorTableFunction
    {
        @Override
        public String getSchema()
        {
            return SCHEMA;
        }

        @Override
        public String getName()
        {
            return TEST_FUNCTION;
        }

        @Override
        public List<ArgumentSpecification> getArguments()
        {
            return null;
        }

        @Override
        public ReturnTypeSpecification getReturnTypeSpecification()
        {
            return ReturnTypeSpecification.OnlyPassThrough.ONLY_PASS_THROUGH;
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return null;
        }
    }

    private static class DuplicateArgumentsTableFunction
            implements ConnectorTableFunction
    {
        @Override
        public String getSchema()
        {
            return SCHEMA;
        }

        @Override
        public String getName()
        {
            return TEST_FUNCTION;
        }

        @Override
        public List<ArgumentSpecification> getArguments()
        {
            ScalarArgumentSpecification s = ScalarArgumentSpecification.builder().name("a").type(INTEGER).build();
            ScalarArgumentSpecification s2 = ScalarArgumentSpecification.builder().name("a").type(INTEGER).build();
            return ImmutableList.of(s, s2);
        }

        @Override
        public ReturnTypeSpecification getReturnTypeSpecification()
        {
            return ReturnTypeSpecification.OnlyPassThrough.ONLY_PASS_THROUGH;
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return null;
        }
    }

    private static class MultipleRTSTableFunction
            implements ConnectorTableFunction
    {
        @Override
        public String getSchema()
        {
            return SCHEMA;
        }

        @Override
        public String getName()
        {
            return TEST_FUNCTION;
        }

        @Override
        public List<ArgumentSpecification> getArguments()
        {
            TableArgumentSpecification t = TableArgumentSpecification.builder().name("t").rowSemantics().build();
            TableArgumentSpecification t2 = TableArgumentSpecification.builder().name("t2").rowSemantics().build();
            return ImmutableList.of(t, t2);
        }

        @Override
        public ReturnTypeSpecification getReturnTypeSpecification()
        {
            return ReturnTypeSpecification.OnlyPassThrough.ONLY_PASS_THROUGH;
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return null;
        }
    }
}
