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

import static com.facebook.presto.SystemSessionProperties.LEGACY_JOIN_USING;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestLegacyJoinUsing
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions(testSessionBuilder()
                .setSystemProperty(LEGACY_JOIN_USING, "true")
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void test()
    {
        assertions.assertQuery(
                "SELECT * FROM (VALUES (1, 'a')) t(k, v1) JOIN (VALUES (1, 'b')) u(k, v2) USING (k)",
                "VALUES (1, 'a', 1, 'b')");

        assertions.assertQuery(
                "SELECT * FROM (VALUES (1, 'a')) t(k, v1) LEFT JOIN (VALUES (2, 'b')) u(k, v2) USING (k)",
                "VALUES (1, 'a', CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR(1)))");

        assertions.assertQuery(
                "SELECT * FROM (VALUES (1, 'a')) t(k, v1) RIGHT JOIN (VALUES (2, 'b')) u(k, v2) USING (k)",
                "VALUES (CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR(1)), 2, 'b')");

        assertions.assertQuery(
                "SELECT * FROM (VALUES (1, 'a')) t(k, v1) FULL OUTER JOIN (VALUES (2, 'b')) u(k, v2) USING (k)",
                "VALUES " +
                        "(CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR(1)), 2, 'b')," +
                        "(1, 'a', CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR(1)))");
    }

    @Test
    public void testCoercions()
    {
        // long, double
        assertions.assertQuery(
                "SELECT * FROM (VALUES (1, 2e0)) x (a, b) JOIN (VALUES (DOUBLE '1.0', 3)) y (a, b) USING(a)",
                "VALUES (1, 2e0, 1e0, 3)");

        // double, long
        assertions.assertQuery(
                "SELECT * FROM (VALUES (1.0E0, 2e0)) x (a, b) JOIN (VALUES (1, 3)) y (a, b) USING(a)",
                "VALUES (1e0, 2e0, 1, 3)");

        // long decimal, bigint
        assertions.assertQuery(
                "SELECT * FROM (VALUES (DECIMAL '0000000000000000001', 2e0)) x (a, b) JOIN (VALUES (1, 3)) y (a, b) USING(a)",
                "VALUES (DECIMAL '1', 2e0, 1, 3)");

        // bigint, long decimal
        assertions.assertQuery(
                "SELECT * FROM (VALUES (1, 2e0)) x (a, b) JOIN (VALUES (DECIMAL '0000000000000000001', 3)) y (a, b) USING(a)",
                "VALUES (1, 2e0, DECIMAL '1', 3)");

        // bigint, short decimal
        assertions.assertQuery(
                "SELECT * FROM (VALUES (1, 2e0)) x (a, b) JOIN (VALUES (1e0, 3)) y (a, b) USING(a)",
                "VALUES (1, 2e0, 1e0, 3)");

        // short decimal, bigint
        assertions.assertQuery(
                "SELECT * FROM (VALUES (1e0, 2e0)) x (a, b) JOIN (VALUES (1, 3)) y (a, b) USING(a)",
                "VALUES (1e0, 2e0, 1, 3)");

        assertions.assertQuery(
                "SELECT * FROM (VALUES (1, 2)) x (a, b) JOIN (VALUES (CAST (1 AS SMALLINT), CAST(3 AS SMALLINT))) y (a, b) USING(a)",
                "VALUES (1, 2, SMALLINT '1', SMALLINT '3')");
    }
}
