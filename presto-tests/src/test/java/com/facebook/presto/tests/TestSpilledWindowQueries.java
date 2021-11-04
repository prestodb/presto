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

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_REVOCABLE_MEMORY_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.WINDOW_SPILL_ENABLED;

public class TestSpilledWindowQueries
        extends AbstractTestWindowQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TestDistributedSpilledQueries.localCreateQueryRunner();
    }

    @Test
    public void testDoesNotSpillWhenWindowSpillDisabled()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(WINDOW_SPILL_ENABLED, "false")
                // set this low so that if we ran with spill the query would fail
                .setSystemProperty(QUERY_MAX_REVOCABLE_MEMORY_PER_NODE, "1B")
                .build();

        assertQuery(session,
                "SELECT orderkey, orderstatus " +
                        ", row_number() OVER (ORDER BY orderkey * 2) * " +
                        "  row_number() OVER (ORDER BY orderkey DESC) + 100 " +
                        "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) x " +
                        "ORDER BY orderkey LIMIT 5",
                "VALUES " +
                        "(1, 'O', 110), " +
                        "(2, 'O', 118), " +
                        "(3, 'F', 124), " +
                        "(4, 'O', 128), " +
                        "(5, 'F', 130)");
    }
}
