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

public class TestUnnest
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
    public void testUnnestArrayRows()
    {
        assertions.assertQuery(
                "SELECT * FROM UNNEST(ARRAY[ROW(1, 1.1), ROW(3, 3.3)], ARRAY[ROW('a', true), ROW('b', false)])",
                "VALUES (1, 1.1, 'a', true), (3, 3.3, 'b', false)");
        assertions.assertQuery(
                "SELECT x, y FROM (VALUES (ARRAY[ROW(1.0, 2), ROW(3, 4.123)])) AS t(a) CROSS JOIN UNNEST(a) t(x, y)",
                "VALUES (1.0, 2), (3, 4.123)");
        assertions.assertQuery(
                "SELECT x, y, z FROM (VALUES (ARRAY[ROW(1, 2), ROW(3, 4)])) t(a) CROSS JOIN (VALUES (1), (2)) s(z) CROSS JOIN UNNEST(a) t(x, y)",
                "VALUES (1, 2, 1), (1, 2, 2), (3, 4, 1), (3, 4, 2)");
    }

    @Test
    public void testUnnestPreserveColumnName()
    {
        assertions.assertQuery(
                "SELECT x FROM UNNEST(CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(x int, y varchar))))",
                "VALUES (1), (2)");

        assertions.assertFails(
                "SELECT x FROM" +
                        "(VALUES (3)) AS t(x)" +
                        "CROSS JOIN UNNEST(CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(x int, y varchar))))",
                ".*Column 'x' is ambiguous.*");

        assertions.assertQuery(
                "SELECT t.x FROM" +
                        "(VALUES (3)) AS t(x)" +
                        "CROSS JOIN UNNEST(CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(x int, y varchar))))",
                "VALUES (3), (3)");

        assertions.assertQuery(
                "SELECT u.x FROM" +
                        "(VALUES (3)) AS t(x)" +
                        "CROSS JOIN UNNEST(CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(x int, y varchar)))) u",
                "VALUES (1), (2)");
    }

    @Test
    public void testUnnestMultiExpr()
    {
        assertions.assertFails(
                "SELECT x " +
                        "FROM UNNEST(" +
                        "   CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(x int, y varchar)))," +
                        "   CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(x int, y varchar))))",
                ".*Column 'x' is ambiguous.*");

        assertions.assertQuery(
                "SELECT t3 " +
                        "FROM UNNEST(" +
                        "   CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(x int, y varchar)))," +
                        "   CAST(ARRAY[ROW(3, 'c'), ROW(4, 'd')] as ARRAY(ROW(x int, y varchar)))) t(t1,t2,t3,t4)",
                "VALUES (3), (4)");

        assertions.assertQuery(
                "SELECT x " +
                        "FROM UNNEST(" +
                        "   CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] as ARRAY(ROW(a int, b varchar)))," +
                        "   CAST(ARRAY[ROW(3, 'c'), ROW(4, 'd')] as ARRAY(ROW(x int, y varchar))))",
                "VALUES (3), (4)");
    }
}
