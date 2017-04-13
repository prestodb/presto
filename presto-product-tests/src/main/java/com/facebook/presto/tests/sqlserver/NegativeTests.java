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
package com.facebook.presto.tests.sqlserver;

import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.configuration.Configuration;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.facebook.presto.tests.TestGroups.SQL_SERVER;
import static com.facebook.presto.tests.sqlserver.SqlServerTpchTableDefinitions.NATION;
import static com.facebook.presto.tests.sqlserver.TestConstants.CONNECTOR_NAME;
import static com.facebook.presto.tests.sqlserver.TestConstants.KEY_SPACE;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.fulfillment.table.TableRequirements.immutableTable;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.lang.String.format;

public class NegativeTests
        extends ProductTest
        implements RequirementsProvider
{
    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return immutableTable(NATION);
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testNonExistentTable()
            throws SQLException
    {
        String tableName = format("%s.%s.%s", CONNECTOR_NAME, KEY_SPACE, "bogus");
        assertThat(() -> query(format("SELECT * FROM %s", tableName)))
                .failsWithMessage(format("Table %s does not exist", tableName));
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testNonExistentSchema()
            throws SQLException
    {
        String tableName = format("%s.%s.%s", CONNECTOR_NAME, "does_not_exist", "bogus");
        assertThat(() -> query(format("SELECT * FROM %s", tableName)))
                .failsWithMessage("Schema does_not_exist does not exist");
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testNonExistentColumn()
            throws SQLException
    {
        String tableName = format("%s.%s.%s", CONNECTOR_NAME, KEY_SPACE, NATION.getName());
        assertThat(() -> query(format("SELECT bogus FROM %s", tableName)))
                .failsWithMessage("Column 'bogus' cannot be resolved");
    }
}
