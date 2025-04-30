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
package com.facebook.presto.tests;

import com.facebook.presto.connector.MockConnectorFactory;
import com.facebook.presto.connector.MockConnectorPlugin;
import com.facebook.presto.connector.TestingTableFunctions.SimpleTableFunction;
import com.facebook.presto.connector.TestingTableFunctions.SimpleTableFunction.SimpleTableFunctionHandle;
import com.facebook.presto.spi.connector.TableFunctionApplicationResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestTableFunctionInvocation
        extends AbstractTestQueryFramework
{
    private static final String TESTING_CATALOG = "testing_catalog1";
    private static final String TABLE_FUNCTION_SCHEMA = "table_function_schema";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(TESTING_CATALOG)
                        .setSchema(TABLE_FUNCTION_SCHEMA)
                        .build())
                .build();
    }

    @BeforeClass
    public void setUp()
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();

        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withTableFunctions(ImmutableSet.of(new SimpleTableFunction()))
                .withApplyTableFunction((session, handle) -> {
                    if (handle instanceof SimpleTableFunctionHandle) {
                        SimpleTableFunctionHandle functionHandle = (SimpleTableFunctionHandle) handle;
                        return Optional.of(new TableFunctionApplicationResult<>(functionHandle.getTableHandle(), functionHandle.getTableHandle().getColumns().orElseThrow(() -> new IllegalStateException("Columns are missing"))));
                    }
                    throw new IllegalStateException("Unsupported table function handle: " + handle.getClass().getSimpleName());
                })
                .build()));
        queryRunner.createCatalog(TESTING_CATALOG, "mock");
    }
/*
    @Test
    public void testPrimitiveDefaultArgument()
    {
        assertQuery("SELECT boolean_column FROM TABLE(system.simple_table_function(column => 'boolean_column', ignored => 1))", "SELECT true WHERE false");

        // skip the `ignored` argument.
        assertQuery("SELECT boolean_column FROM TABLE(system.simple_table_function(column => 'boolean_column'))",
                "SELECT true WHERE false");
    }
 */

    @Test
    public void testNoArgumentsPassed()
    {
        assertQuery("SELECT col FROM TABLE(system.simple_table_function())",
                  "SELECT true WHERE false");
    }
}
