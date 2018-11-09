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
package com.facebook.presto.sql.query;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(QueryAssertionsExtension.class)
public class TestGroupingSets
{
    @Test
    public void testPredicateOverGroupingKeysWithEmptyGroupingSet(QueryAssertions assertions)
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
    public void testDistinctWithMixedReferences(QueryAssertions assertions)
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
