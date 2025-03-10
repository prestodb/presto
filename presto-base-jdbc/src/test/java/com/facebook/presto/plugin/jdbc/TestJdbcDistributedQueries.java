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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueries;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.jdbc.JdbcQueryRunner.createJdbcQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestJdbcDistributedQueries
        extends AbstractTestQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createJdbcQueryRunner(TpchTable.getTables());
    }

    @Override
    public void testLargeIn()
    {
    }

    @Test
    public void testNativeQueryParameters()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query_simple", "SELECT * FROM TABLE(system.query(query => ?))")
                .addPreparedStatement("my_query", "SELECT * FROM TABLE(system.query(query => format('SELECT %s FROM %s', ?, ?)))")
                .build();
        assertQueryFails(session, "EXECUTE my_query_simple USING 'SELECT 1 a'", "line 1:21: Table function system.query not registered");
        assertQueryFails(session, "EXECUTE my_query USING 'a', '(SELECT 2 a) t'", "line 1:21: Table function system.query not registered");
    }
}
