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

import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestWindowQueries;
import org.testng.annotations.Test;

public class TestWindowQueries
        extends AbstractTestWindowQueries
{
    private String frameTypeDiffersError = ".*Window frame of type RANGE does not match types of the ORDER BY and frame column.*";
    private String unsupportedWindowTypeError = ".*Unsupported window type: 2.*";
    private String invalidOffsetError = ".*Window frame offset value must not be negative or null.*";
    private String functionNotRegisteredError = ".*Scalar function presto.default.plus not registered with arguments.*";

    private String storageFormat = "PARQUET";

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(true, storageFormat);
    }

    @Override
    protected void createTables()
    {
        try {
            QueryRunner javaQueryRunner = PrestoNativeQueryRunnerUtils.createJavaQueryRunner("PARQUET");
            NativeQueryRunnerUtils.createAllTables(javaQueryRunner, false);
            javaQueryRunner.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @Test
    public void testAllPartitionSameValuesGroup()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                        "FROM (VALUES 'a', 'a', 'a') T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES 'a', 'a', 'a') T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES 'a', 'a', 'a') T(a)",
                unsupportedWindowTypeError);

        // test frame bounds at partition bounds
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 10 PRECEDING AND 10 FOLLOWING) " +
                        "FROM (VALUES 'a', 'a', 'a') T(a)",
                unsupportedWindowTypeError);
    }

    @Override
    @Test
    public void testConstantOffset()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 PRECEDING AND 2 FOLLOWING) " +
                        "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS CURRENT ROW) " +
                        "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 0 PRECEDING AND 0 FOLLOWING) " +
                        "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                        "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 2 FOLLOWING AND 1 FOLLOWING) " +
                        "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) T(a)",
                unsupportedWindowTypeError);
    }

    @Override
    @Test
    public void testEmptyFrameRealBounds()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 0.5 FOLLOWING AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, 2, 4) T(a)", frameTypeDiffersError, true);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a NULLS FIRST RANGE BETWEEN 2 PRECEDING AND 1.5 PRECEDING) " +
                        "FROM (VALUES null, 1, 2) T(a)",
                frameTypeDiffersError, true);
    }

    @Override
    @Test
    public void testEmptyFrameGroup()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN 90 PRECEDING AND 100 PRECEDING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN 100 FOLLOWING AND 90 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                unsupportedWindowTypeError);
    }

    // TODO: This test is flaky so disabled for now.
    @Override
    @Test(enabled = false)
    public void testInvalidOffset()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC RANGE x PRECEDING) " +
                        "FROM (VALUES (1, 0.1), (2, -0.2)) T(a, x)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC RANGE BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 0.1), (2, -0.2)) T(a, x)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE x PRECEDING) " +
                        "FROM (VALUES (1, 0.1), (2, -0.2)) T(a, x)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 0.1), (2, -0.2)) T(a, x)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE x PRECEDING) " +
                        "FROM (VALUES (1, 0.1), (2, null)) T(a, x)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 0.1), (2, null)) T(a, x)",
                frameTypeDiffersError);

        // fail if offset is invalid for null sort key
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 0.1), (null, null)) T(a, x)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC RANGE BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 0.1), (null, -0.1)) T(a, x)",
                invalidOffsetError);

        // test invalid offset of different types
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (1, BIGINT '-1')) T(a, x)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (1, INTEGER '-1')) T(a, x)",
                invalidOffsetError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (SMALLINT '1', SMALLINT '-1')) T(a, x)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (TINYINT '1', TINYINT '-1')) T(a, x)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (1, -1.1e0)) T(a, x)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (1, REAL '-1.1')) T(a, x)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (1, -1.0001)) T(a, x)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' YEAR)) T(a, x)",
                functionNotRegisteredError, true);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' MONTH)) T(a, x)",
                functionNotRegisteredError, true);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' DAY)) T(a, x)",
                functionNotRegisteredError, true);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' HOUR)) T(a, x)",
                functionNotRegisteredError, true);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' MINUTE)) T(a, x)",
                functionNotRegisteredError, true);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE x PRECEDING) " +
                        "FROM (VALUES (DATE '2001-01-31', INTERVAL '-1' SECOND)) T(a, x)",
                functionNotRegisteredError, true);
    }

    @Override
    @Test
    public void testInvalidOffsetGroup()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC GROUPS x PRECEDING) " +
                        "FROM (VALUES (1, 1), (2, -2)) T(a, x)",
                unsupportedWindowTypeError);
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC GROUPS x PRECEDING) " +
                        "FROM (VALUES (1, 1), (2, -2)) T(a, x)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC GROUPS BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 1), (2, -2)) T(a, x)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS x PRECEDING) " +
                        "FROM (VALUES (1, 1), (2, -2)) T(a, x)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 1), (2, -2)) T(a, x)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS x PRECEDING) " +
                        "FROM (VALUES (1, 1), (2, null)) T(a, x)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 1), (2, null)) T(a, x)",
                unsupportedWindowTypeError);

        // fail if offset is invalid for null sort key
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 1), (null, null)) T(a, x)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC GROUPS BETWEEN 1 PRECEDING AND x FOLLOWING) " +
                        "FROM (VALUES (1, 1), (null, -1)) T(a, x)",
                unsupportedWindowTypeError);

        // test invalid offset of different types
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS x PRECEDING) " +
                        "FROM (VALUES (1, BIGINT '-1')) T(a, x)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS x PRECEDING) " +
                        "FROM (VALUES (1, INTEGER '-1')) T(a, x)",
                unsupportedWindowTypeError);
    }

    @Override
    @Test
    public void testMixedTypeFrameBounds()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST GROUPS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                unsupportedWindowTypeError);
    }

    @Override
    @Test
    public void testMixedTypeFrameBoundsAscendingNullsFirst()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 PRECEDING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN CURRENT ROW AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN 0.5 PRECEDING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST RANGE BETWEEN 0.5 FOLLOWING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);
    }

    @Override
    @Test
    public void testMixedTypeFrameBoundsAscendingNullsLast()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 PRECEDING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN CURRENT ROW AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 0.5 PRECEDING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS LAST RANGE BETWEEN 0.5 FOLLOWING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);
    }

    @Override
    @Test
    public void testMixedTypeFrameBoundsDescendingNullsFirst()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 PRECEDING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN CURRENT ROW AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN 1.5 PRECEDING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS FIRST RANGE BETWEEN 1.5 FOLLOWING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);
    }

    @Override
    @Test
    public void testMixedTypeFrameBoundsDescendingNullsLast()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 PRECEDING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN CURRENT ROW AND 1.5 FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 0.5 PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 0.5 PRECEDING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 1.5 FOLLOWING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                frameTypeDiffersError);
    }

    @Override
    @Test
    public void testMultipleWindowFunctionsGroup()
    {
        // two functions with frame type GROUPS
        assertQueryFails("SELECT x, array_agg(date) OVER(ORDER BY x GROUPS BETWEEN 1 PRECEDING AND 1 PRECEDING), avg(number) OVER(ORDER BY x GROUPS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) " +
                        "FROM (VALUES " +
                        "(2, DATE '2222-01-01', 4.4), " +
                        "(1, DATE '1111-01-01', 2.2), " +
                        "(3, DATE '3333-01-01', 6.6)) T(x, date, number)",
                unsupportedWindowTypeError);

        // three functions with different frame types
        assertQueryFails("SELECT " +
                        "x, " +
                        "array_agg(a) OVER(ORDER BY x RANGE BETWEEN 2 PRECEDING AND CURRENT ROW), " +
                        "array_agg(a) OVER(ORDER BY x GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING), " +
                        "array_agg(a) OVER(ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES " +
                        "(1.0, 1), " +
                        "(2.0, 2), " +
                        "(3.0, 3), " +
                        "(4.0, 4), " +
                        "(5.0, 5), " +
                        "(6.0, 6)) T(x, a)",
                unsupportedWindowTypeError);
    }

    @Override
    public void testNonConstantOffset()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN x * 10 PRECEDING AND y / 10.0 FOLLOWING) " +
                        "FROM (VALUES (1, 0.1, 10), (2, 0.2, 20), (4, 0.4, 40)) T(a, x, y)",
                frameTypeDiffersError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN x * 10 PRECEDING AND y / 10.0 FOLLOWING) " +
                        "FROM (VALUES (1, 0.1, 10), (2, 0.2, 20), (4, 0.4, 40), (null, 0.5, 50)) T(a, x, y)",
                frameTypeDiffersError);
    }

    @Override
    @Test
    public void testNonConstantOffsetGroup()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN x PRECEDING AND y FOLLOWING) " +
                        "FROM (VALUES ('a', 1, 1), ('b', 2, 0), ('c', 0, 3)) T(a, x, y)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN x FOLLOWING AND y FOLLOWING) " +
                        "FROM (VALUES ('a', 1, 1), ('b', 2, 0), ('c', 3, 3), ('d', 0, 0)) T(a, x, y)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN x PRECEDING AND y PRECEDING) " +
                        "FROM (VALUES ('a', 1, 1), ('b', 0, 2), ('c', 2, 1), ('d', 0, 2)) T(a, x, y)",
                unsupportedWindowTypeError);
    }

    @Override
    @Test
    public void testNoValueFrameBoundsGroup()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) " +
                        "FROM (VALUES 1, null, null, 2, 1) T(a)",
                unsupportedWindowTypeError);
    }

    @Override
    @Test
    public void testOnlyNullsGroup()
    {
        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 1 PRECEDING AND 2 FOLLOWING) " +
                        "FROM (VALUES CAST(null AS integer), null, null) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                        "FROM (VALUES CAST(null AS integer), null, null) T(a)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES CAST(null AS integer), null, null) T(a)",
                unsupportedWindowTypeError);
    }

    @Override
    @Test
    public void testWindowPartitioning()
    {
        assertQueryFails("SELECT a, p, array_agg(a) OVER(PARTITION BY p ORDER BY a ASC NULLS FIRST RANGE BETWEEN 0.5 PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES (1, 'x'), (2, 'x'), (null, 'x'), (null, 'y'), (2, 'y')) T(a, p)",
                frameTypeDiffersError);

        assertQueryFails("SELECT a, p, array_agg(a) OVER(PARTITION BY p ORDER BY a ASC NULLS FIRST RANGE BETWEEN 0.5 PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES (1, 'x'), (2, 'x'), (null, 'x'), (null, 'y'), (2, 'y'), (null, null), (null, null), (1, null)) T(a, p)",
                frameTypeDiffersError);
    }

    @Override
    @Test
    public void testWindowPartitioningGroup()
    {
        assertQueryFails("SELECT a, p, array_agg(a) OVER(PARTITION BY p ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES (1, 'x'), (2, 'x'), (null, 'x'), (null, 'y'), (2, 'y')) T(a, p)",
                unsupportedWindowTypeError);

        assertQueryFails("SELECT a, p, array_agg(a) OVER(PARTITION BY p ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 0 PRECEDING AND 1 FOLLOWING) " +
                        "FROM (VALUES (1, 'x'), (2, 'x'), (null, 'x'), (null, 'y'), (2, 'y'), (null, null), (null, null), (1, null)) T(a, p)",
                unsupportedWindowTypeError);
    }

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
        assertQueryFails("SELECT x, array_agg(x) OVER(ORDER BY x RANGE BETWEEN interval '1' year PRECEDING AND interval '1' month FOLLOWING) " +
                        "FROM (VALUES " +
                        "INTERVAL '1' month, " +
                        "INTERVAL '2' month, " +
                        "INTERVAL '5' year) T(x)",
                functionNotRegisteredError, true);
    }

    @Override
    @Test
    public void testEmptyFrameIntegralBounds()
    {
        assertQuery("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 10 PRECEDING) " +
                        "FROM (VALUES 1, 2, 3, null, null, 2, 1, null, null) T(a)",
                "VALUES " +
                        "CAST(null AS array), " +
                        "null, " +
                        "null, " +
                        "null, " +
                        "null, " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null]");

        assertQuery("SELECT array_agg(a) OVER(ORDER BY a DESC NULLS LAST RANGE BETWEEN 10 FOLLOWING AND 1 FOLLOWING) " +
                        "FROM (VALUES 1, 2, 3, null, null, 2, 1, null, null) T(a)",
                "VALUES " +
                        "CAST(null AS array), " +
                        "null, " +
                        "null, " +
                        "null, " +
                        "null, " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null], " +
                        "ARRAY[null, null, null, null]");

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                        "FROM (VALUES 1.0, 1.1) T(a)", frameTypeDiffersError, true);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a NULLS LAST RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                        "FROM (VALUES 1.0, 1.1, null) T(a)", frameTypeDiffersError, true);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES 1.0, 1.1) T(a)", frameTypeDiffersError, true);

        assertQueryFails("SELECT array_agg(a) OVER(ORDER BY a NULLS FIRST RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES null, 1.0, 1.1) T(a)", frameTypeDiffersError, true);

        assertQuery("SELECT array_agg(a) OVER(ORDER BY a RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES 1, 2) T(a)",
                "VALUES " +
                        "null, " +
                        "ARRAY[1]");

        assertQuery("SELECT array_agg(a) OVER(ORDER BY a NULLS FIRST RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                        "FROM (VALUES null, 1, 2) T(a)",
                "VALUES " +
                        "ARRAY[null], " +
                        "null, " +
                        "ARRAY[1]");
    }

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
                        "(6.0, 6)) T(x, a)", frameTypeDiffersError, true);
    }
}
