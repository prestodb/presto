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

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestFilteredAggregations
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
    }

    @Test
    public void testGroupAll()
    {
        assertions.assertQuery(
                "SELECT count(DISTINCT x) FILTER (WHERE x > 1) " +
                        "FROM (VALUES 1, 1, 1, 2, 3, 3) t(x)",
                "VALUES BIGINT '2'");

        assertions.assertQuery(
                "SELECT count(DISTINCT x) FILTER (WHERE x > 1), sum(DISTINCT x) " +
                        "FROM (VALUES 1, 1, 1, 2, 3, 3) t(x)",
                "VALUES (BIGINT '2', BIGINT '6')");

        assertions.assertQuery(
                "SELECT count(DISTINCT x) FILTER (WHERE x > 1), sum(DISTINCT y) FILTER (WHERE x < 3)" +
                        "FROM (VALUES " +
                        "(1, 10)," +
                        "(1, 20)," +
                        "(1, 20)," +
                        "(2, 20)," +
                        "(3, 30)) t(x, y)",
                "VALUES (BIGINT '2', BIGINT '30')");

        assertions.assertQuery(
                "SELECT count(x) FILTER (WHERE x > 1), sum(DISTINCT x) " +
                        "FROM (VALUES 1, 2, 3, 3) t(x)",
                "VALUES (BIGINT '3', BIGINT '6')");
    }

    @Test
    public void testGroupingSets()
    {
        assertions.assertQuery(
                "SELECT k, count(DISTINCT x) FILTER (WHERE y = 100), count(DISTINCT x) FILTER (WHERE y = 200) FROM " +
                        "(VALUES " +
                        "   (1, 1, 100)," +
                        "   (1, 1, 200)," +
                        "   (1, 2, 100)," +
                        "   (1, 3, 300)," +
                        "   (2, 1, 100)," +
                        "   (2, 10, 100)," +
                        "   (2, 20, 100)," +
                        "   (2, 20, 200)," +
                        "   (2, 30, 300)," +
                        "   (2, 40, 100)" +
                        ") t(k, x, y) " +
                        "GROUP BY GROUPING SETS ((), (k))",
                "VALUES " +
                        "(1, BIGINT '2', BIGINT '1'), " +
                        "(2, BIGINT '4', BIGINT '1'), " +
                        "(CAST(NULL AS INTEGER), BIGINT '5', BIGINT '2')");
    }
}
