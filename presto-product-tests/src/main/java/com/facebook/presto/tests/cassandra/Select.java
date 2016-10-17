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
import com.teradata.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static com.facebook.presto.tests.TestGroups.CASSANDRA;
import static com.facebook.presto.tests.TpchTableResults.PRESTO_NATION_RESULT;
import static com.facebook.presto.tests.cassandra.CassandraTpchTableDefinitions.CASSANDRA_NATION;
import static com.facebook.presto.tests.cassandra.TestConstants.CONNECTOR_NAME;
import static com.facebook.presto.tests.cassandra.TestConstants.KEY_SPACE;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.fulfillment.table.TableRequirements.immutableTable;
import static java.lang.String.format;

public class Select
        extends ProductTest
        implements RequirementsProvider
{
    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return immutableTable(CASSANDRA_NATION);
    }

    @Test(groups = CASSANDRA)
    public void testSelectNation()
            throws SQLException
    {
        String sql = format(
                "SELECT n_nationkey, n_name, n_regionkey, n_comment FROM %s.%s.%s",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_NATION.getName());
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).matches(PRESTO_NATION_RESULT);
    }
}
