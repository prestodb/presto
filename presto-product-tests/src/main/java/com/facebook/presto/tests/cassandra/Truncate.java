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
package com.facebook.presto.tests.cassandra;

import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.configuration.Configuration;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.CASSANDRA;
import static com.facebook.presto.tests.cassandra.CassandraTpchTableDefinitions.CASSANDRA_NATION;
import static com.facebook.presto.tests.cassandra.TestConstants.CONNECTOR_NAME;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static com.teradata.tempto.fulfillment.table.TableRequirements.mutableTable;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.lang.String.format;

public class Truncate
        extends ProductTest
        implements RequirementsProvider
{
    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return mutableTable(CASSANDRA_NATION);
    }

    @Test(groups = CASSANDRA)
    public void truncateTable()
    {
        String tableName = format("%s.%s", CONNECTOR_NAME, mutableTablesState().get(CASSANDRA_NATION.getName()).getNameInDatabase());
        assertThat(query(format("SELECT * FROM %s", tableName))).hasRowsCount(25);
        query(format("TRUNCATE TABLE %s", tableName));
        assertThat(query(format("SELECT * FROM %s", tableName))).hasRowsCount(0);
    }
}
