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
import static com.facebook.presto.tests.cassandra.MultiColumnKeyTableDefinition.CASSANDRA_MULTI_COLUMN_KEY;
import static com.facebook.presto.tests.cassandra.TestConstants.CONNECTOR_NAME;
import static com.facebook.presto.tests.cassandra.TestConstants.KEY_SPACE;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.fulfillment.table.TableRequirements.immutableTable;
import static java.lang.String.format;

public class SelectMultiColumnKey
        extends ProductTest
        implements RequirementsProvider
{
    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return immutableTable(CASSANDRA_MULTI_COLUMN_KEY);
    }

    @Test(groups = CASSANDRA)
    public void testSelectWithEqualityFilterOnClusteringKey()
            throws SQLException
    {
        String sql = format(
                "SELECT value FROM %s.%s.%s WHERE key = 'a1'",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_MULTI_COLUMN_KEY.getName());
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row("Test value 1"));
    }

    @Test(groups = CASSANDRA)
    public void testSelectWithEqualityFilterOnPrimaryAndClusteringKeys()
            throws SQLException
    {
        String sql = format(
                "SELECT value FROM %s.%s.%s WHERE user_id = 'Alice' and key = 'a1' and updated_at = TIMESTAMP '2015-01-01 01:01:01'",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_MULTI_COLUMN_KEY.getName());
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row("Test value 1"));
    }

    @Test(groups = CASSANDRA)
    public void testSelectWithMixedFilterOnPrimaryAndClusteringKeys()
            throws SQLException
    {
        String sql = format(
                "SELECT value FROM %s.%s.%s WHERE user_id = 'Alice' and key < 'b' and updated_at >= TIMESTAMP '2015-01-01 01:01:01'",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_MULTI_COLUMN_KEY.getName());
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row("Test value 1"));
    }

    @Test(groups = CASSANDRA)
    public void testSelectWithFilterOnPrimaryKeyNoMatch()
            throws SQLException
    {
        String sql = format(
                "SELECT value FROM %s.%s.%s WHERE user_id = 'George'",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_MULTI_COLUMN_KEY.getName());
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).hasNoRows();
    }

    @Test(groups = CASSANDRA)
    public void testSelectWithFilterOnPrefixOfClusteringKey()
            throws SQLException
    {
        String sql = format(
                "SELECT value FROM %s.%s.%s WHERE user_id = 'Bob' and key = 'b1'",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_MULTI_COLUMN_KEY.getName());
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row("Test value 2"));
    }

    @Test(groups = CASSANDRA)
    public void testSelectWithFilterOnSecondClusteringKey()
            throws SQLException
    {
        // Since update_at is the second clustering key, this forces a full table scan.
        String sql = format(
                "SELECT value FROM %s.%s.%s WHERE user_id = 'Bob' and updated_at = TIMESTAMP '2014-02-02 03:04:05'",
                CONNECTOR_NAME,
                KEY_SPACE,
                CASSANDRA_MULTI_COLUMN_KEY.getName());
        QueryResult queryResult = onPresto()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row("Test value 2"));
    }
}
