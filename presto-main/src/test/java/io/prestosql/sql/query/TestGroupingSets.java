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
package io.prestosql.sql.query;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestGroupingSets
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testPredicateOverGroupingKeysWithEmptyGroupingSet()
    {
        assertions.assertQuery(
                "WITH t AS (" +
                        "    SELECT a" +
                        "    FROM (" +
                        "        VALUES 1, 2" +
                        "    ) AS u(a)" +
                        "    GROUP BY GROUPING SETS ((), (a))" +
                        ")" +
                        "SELECT * " +
                        "FROM t " +
                        "WHERE a IS NOT NULL",
                "VALUES 1, 2");
    }

    @Test
    public void testDistinctWithMixedReferences()
    {
        assertions.assertQuery("" +
                        "SELECT a " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY DISTINCT ROLLUP(a, t.a)",
                "VALUES (1), (NULL)");

        assertions.assertQuery("" +
                        "SELECT a " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY DISTINCT GROUPING SETS ((a), (t.a))",
                "VALUES 1");

        assertions.assertQuery("" +
                        "SELECT a " +
                        "FROM (VALUES 1) t(a) " +
                        "GROUP BY DISTINCT a, GROUPING SETS ((), (t.a))",
                "VALUES 1");
    }
}
