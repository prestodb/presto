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
package com.facebook.presto.nativetests;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestWindowQueries;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.lang.Boolean.parseBoolean;

public class TestWindowQueries
        extends AbstractTestWindowQueries
{
    private static final String frameTypeDiffersError = ".*Window frame of type RANGE does not match types of the ORDER BY and frame column.*";

    private String storageFormat;
    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return NativeTestsUtils.createNativeQueryRunner(storageFormat, sidecarEnabled);
    }

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables(storageFormat);
    }

    /// Queries in this test fail because GROUPS mode in Window frame is not supported in Prestissimo. See issue:
    /// https://github.com/prestodb/presto/issues/24413.
    @Override
    @Test(enabled = false)
    public void testAllPartitionSameValuesGroup() {}

    /// Queries in this test fail because GROUPS mode in Window frame is not supported in Prestissimo. See issue:
    /// https://github.com/prestodb/presto/issues/24413.
    @Override
    @Test(enabled = false)
    public void testConstantOffset() {}

    /// Queries in this test fail because GROUPS mode in Window frame is not supported in Prestissimo. See issue:
    /// https://github.com/prestodb/presto/issues/24413.
    @Override
    @Test(enabled = false)
    public void testEmptyFrameGroup() {}

    /// Queries in this test fail because GROUPS mode in Window frame is not supported in Prestissimo. See issue:
    /// https://github.com/prestodb/presto/issues/24413.
    @Override
    @Test(enabled = false)
    public void testInvalidOffsetGroup() {}

    /// Queries in this test fail because GROUPS mode in Window frame is not supported in Prestissimo. See issue:
    /// https://github.com/prestodb/presto/issues/24413.
    @Override
    @Test(enabled = false)
    public void testMixedTypeFrameBounds() {}

    /// Queries in this test fail because GROUPS mode in Window frame is not supported in Prestissimo. See issue:
    /// https://github.com/prestodb/presto/issues/24413.
    @Override
    @Test(enabled = false)
    public void testMultipleWindowFunctionsGroup() {}

    /// Queries in this test fail because GROUPS mode in Window frame is not supported in Prestissimo. See issue:
    /// https://github.com/prestodb/presto/issues/24413.
    @Override
    @Test(enabled = false)
    public void testNonConstantOffsetGroup() {}

    /// Queries in this test fail because GROUPS mode in Window frame is not supported in Prestissimo. See issue:
    /// https://github.com/prestodb/presto/issues/24413.
    @Override
    @Test(enabled = false)
    public void testNoValueFrameBoundsGroup() {}

    /// Queries in this test fail because GROUPS mode in Window frame is not supported in Prestissimo. See issue:
    /// https://github.com/prestodb/presto/issues/24413.
    @Override
    @Test(enabled = false)
    public void testOnlyNullsGroup() {}

    /// Queries in this test fail because GROUPS mode in Window frame is not supported in Prestissimo. See issue:
    /// https://github.com/prestodb/presto/issues/24413.
    @Override
    @Test(enabled = false)
    public void testWindowPartitioningGroup() {}

    /// This test is flaky so disabled for now, see issue: https://github.com/prestodb/presto/issues/21888.
    @Override
    @Test
    public void testInvalidOffset() {}

    /// Queries in this test fail because the Window's ORDER BY column type differs from the frame bound type. See
    /// issue: https://github.com/prestodb/presto/issues/23269.
    @Override
    @Test(enabled = false)
    public void testEmptyFrameMixedBounds() {}

    /// Queries in this test fail because the Window's ORDER BY column type differs from the frame bound type. See
    /// issue: https://github.com/prestodb/presto/issues/23269.
    @Override
    @Test(enabled = false)
    public void testMixedTypeFrameBoundsAscendingNullsFirst() {}

    /// Queries in this test fail because the Window's ORDER BY column type differs from the frame bound type. See
    /// issue: https://github.com/prestodb/presto/issues/23269.
    @Override
    @Test(enabled = false)
    public void testMixedTypeFrameBoundsAscendingNullsLast() {}

    /// Queries in this test fail because the Window's ORDER BY column type differs from the frame bound type. See
    /// issue: https://github.com/prestodb/presto/issues/23269.
    @Override
    @Test(enabled = false)
    public void testMixedTypeFrameBoundsDescendingNullsFirst() {}

    /// Queries in this test fail because the Window's ORDER BY column type differs from the frame bound type. See
    /// issue: https://github.com/prestodb/presto/issues/23269.
    @Override
    @Test(enabled = false)
    public void testMixedTypeFrameBoundsDescendingNullsLast() {}

    /// Queries in this test fail because the Window's ORDER BY column type differs from the frame bound type. See
    /// issue: https://github.com/prestodb/presto/issues/23269.
    @Override
    @Test(enabled = false)
    public void testNonConstantOffset() {}

    /// Queries in this test fail because the Window's ORDER BY column type differs from the frame bound type. See
    /// issue: https://github.com/prestodb/presto/issues/23269.
    @Override
    @Test(enabled = false)
    public void testWindowPartitioning() {}

    /// The last query in this test fails because the Window's ORDER BY column type differs from the frame bound type.
    /// See issue: https://github.com/prestodb/presto/issues/23269.
    @Override
    @Test
    public void testMultipleWindowFunctions()
    {
        assertQuery("SELECT x, array_agg(date) OVER(ORDER BY x RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING), avg(number) OVER(ORDER BY x RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES " +
                        "(2, DATE '2222-01-01', 4.4), " +
                        "(1, DATE '1111-01-01', 2.2), " +
                        "(3, DATE '3333-01-01', 6.6)) T(x, date, number)",
                "VALUES " +
                        "(1, ARRAY[DATE '1111-01-01', DATE '2222-01-01'], 3.3), " +
                        "(2, ARRAY[DATE '1111-01-01', DATE '2222-01-01', DATE '3333-01-01'], 4.4), " +
                        "(3, ARRAY[DATE '2222-01-01', DATE '3333-01-01'], 5.5)");

        assertQueryFails("SELECT x, array_agg(a) OVER(ORDER BY x RANGE BETWEEN 2 PRECEDING AND CURRENT ROW), array_agg(a) OVER(ORDER BY x RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) " +
                        "FROM (VALUES " +
                        "(1.0, 1), " +
                        "(2.0, 2), " +
                        "(3.0, 3), " +
                        "(4.0, 4), " +
                        "(5.0, 5), " +
                        "(6.0, 6)) T(x, a)", frameTypeDiffersError);
    }

    /// The first query in this test fails because the Window's ORDER BY column type differs from the frame bound type.
    /// See issue: https://github.com/prestodb/presto/issues/23269.
    @Override
    @Test
    public void testTypes()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN DOUBLE '0.5' PRECEDING AND TINYINT '1' FOLLOWING) " +
                        "FROM (VALUES 1, null, 2) T(a)",
                frameTypeDiffersError);

        assertQuery("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 0.5 PRECEDING AND 1.000 FOLLOWING) " +
                        "FROM (VALUES REAL '1', null, 2) T(a)",
                "VALUES " +
                        "ARRAY[CAST('1' AS REAL), CAST('2' AS REAL)], " +
                        "ARRAY[CAST('2' AS REAL)], " +
                        "ARRAY[null]");

        assertQuery("SELECT x, array_agg(x) OVER(ORDER BY x DESC RANGE BETWEEN interval '1' month PRECEDING AND interval '1' month FOLLOWING) " +
                        "FROM (VALUES DATE '2001-01-31', DATE '2001-08-25', DATE '2001-09-25', DATE '2001-09-26') T(x)",
                "VALUES " +
                        "(DATE '2001-09-26', ARRAY[DATE '2001-09-26', DATE '2001-09-25']), " +
                        "(DATE '2001-09-25', ARRAY[DATE '2001-09-26', DATE '2001-09-25', DATE '2001-08-25']), " +
                        "(DATE '2001-08-25', ARRAY[DATE '2001-09-25', DATE '2001-08-25']), " +
                        "(DATE '2001-01-31', ARRAY[DATE '2001-01-31'])");

        // January 31 + 1 month sets the frame bound to the last day of February. March 1 is out of range.
        assertQuery("SELECT x, array_agg(x) OVER(ORDER BY x RANGE BETWEEN CURRENT ROW AND interval '1' month FOLLOWING) " +
                        "FROM (VALUES DATE '2001-01-31', DATE '2001-02-28', DATE '2001-03-01') T(x)",
                "VALUES " +
                        "(DATE '2001-01-31', ARRAY[DATE '2001-01-31', DATE '2001-02-28']), " +
                        "(DATE '2001-02-28', ARRAY[DATE '2001-02-28', DATE '2001-03-01']), " +
                        "(DATE '2001-03-01', ARRAY[DATE '2001-03-01'])");

        // H2 and Presto has some type conversion problem for Interval type, hence use the same query runner for this query
        assertQueryWithSameQueryRunner("SELECT x, array_agg(x) OVER(ORDER BY x RANGE BETWEEN interval '1' year PRECEDING AND interval '1' month FOLLOWING) " +
                        "FROM (VALUES " +
                        "INTERVAL '1' month, " +
                        "INTERVAL '2' month, " +
                        "INTERVAL '5' year) T(x)",
                "VALUES " +
                        "(INTERVAL '1' month, ARRAY[INTERVAL '1' month, INTERVAL '2' month]), " +
                        "(INTERVAL '2' month, ARRAY[INTERVAL '1' month, INTERVAL '2' month]), " +
                        "(INTERVAL '5' year, ARRAY[INTERVAL '5' year])");
    }
}
