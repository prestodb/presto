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
package com.facebook.presto.operator.window;

import com.facebook.presto.testing.MaterializedResult;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;

public class TestAggregateWindowFunction
        extends AbstractTestWindowFunction
{
    @Test
    public void testCountRowsOrdered()
    {
        assertWindowQuery("count(*) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", 1L)
                        .row(5, "F", 2L)
                        .row(6, "F", 3L)
                        .row(33, "F", 4L)
                        .row(1, "O", 1L)
                        .row(2, "O", 2L)
                        .row(4, "O", 3L)
                        .row(7, "O", 4L)
                        .row(32, "O", 5L)
                        .row(34, "O", 6L)
                        .build());
        assertWindowQueryWithNulls("count(*) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", 1L)
                        .row(5L, "F", 2L)
                        .row(null, "F", 4L)
                        .row(null, "F", 4L)
                        .row(34L, "O", 1L)
                        .row(null, "O", 2L)
                        .row(1L, null, 1L)
                        .row(7L, null, 2L)
                        .row(null, null, 4L)
                        .row(null, null, 4L)
                        .build());
    }

    @Test
    public void testCountRowsUnordered()
    {
        assertWindowQuery("count(*) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", 4L)
                        .row(5, "F", 4L)
                        .row(6, "F", 4L)
                        .row(33, "F", 4L)
                        .row(1, "O", 6L)
                        .row(2, "O", 6L)
                        .row(4, "O", 6L)
                        .row(7, "O", 6L)
                        .row(32, "O", 6L)
                        .row(34, "O", 6L)
                        .build());
        assertWindowQueryWithNulls("count(*) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", 4L)
                        .row(5L, "F", 4L)
                        .row(null, "F", 4L)
                        .row(null, "F", 4L)
                        .row(34L, "O", 2L)
                        .row(null, "O", 2L)
                        .row(1L, null, 4L)
                        .row(7L, null, 4L)
                        .row(null, null, 4L)
                        .row(null, null, 4L)
                        .build());
    }

    @Test
    public void testCountValuesOrdered()
    {
        assertWindowQuery("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", 1L)
                        .row(5, "F", 2L)
                        .row(6, "F", 3L)
                        .row(33, "F", 4L)
                        .row(1, "O", 1L)
                        .row(2, "O", 2L)
                        .row(4, "O", 3L)
                        .row(7, "O", 4L)
                        .row(32, "O", 5L)
                        .row(34, "O", 6L)
                        .build());
        assertWindowQueryWithNulls("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", 1L)
                        .row(5L, "F", 2L)
                        .row(null, "F", 2L)
                        .row(null, "F", 2L)
                        .row(34L, "O", 1L)
                        .row(null, "O", 1L)
                        .row(1L, null, 1L)
                        .row(7L, null, 2L)
                        .row(null, null, 2L)
                        .row(null, null, 2L)
                        .build());
    }

    @Test
    public void testCountValuesUnordered()
    {
        assertWindowQuery("count(orderkey) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", 4L)
                        .row(5, "F", 4L)
                        .row(6, "F", 4L)
                        .row(33, "F", 4L)
                        .row(1, "O", 6L)
                        .row(2, "O", 6L)
                        .row(4, "O", 6L)
                        .row(7, "O", 6L)
                        .row(32, "O", 6L)
                        .row(34, "O", 6L)
                        .build());
        assertWindowQueryWithNulls("count(orderkey) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", 2L)
                        .row(5L, "F", 2L)
                        .row(null, "F", 2L)
                        .row(null, "F", 2L)
                        .row(34L, "O", 1L)
                        .row(null, "O", 1L)
                        .row(1L, null, 2L)
                        .row(7L, null, 2L)
                        .row(null, null, 2L)
                        .row(null, null, 2L)
                        .build());
    }

    @Test
    public void testSumOrdered()
    {
        MaterializedResult expected = resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                .row(3, "F", 3L)
                .row(5, "F", 8L)
                .row(6, "F", 14L)
                .row(33, "F", 47L)
                .row(1, "O", 1L)
                .row(2, "O", 3L)
                .row(4, "O", 7L)
                .row(7, "O", 14L)
                .row(32, "O", 46L)
                .row(34, "O", 80L)
                .build();
        MaterializedResult expectedNulls = resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                .row(3L, "F", 3L)
                .row(5L, "F", 8L)
                .row(null, "F", 8L)
                .row(null, "F", 8L)
                .row(34L, "O", 34L)
                .row(null, "O", 34L)
                .row(1L, null, 1L)
                .row(7L, null, 8L)
                .row(null, null, 8L)
                .row(null, null, 8L)
                .build();

        // default window frame
        @Language("SQL") String sql = "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)";
        assertWindowQuery(sql, expected);
        assertWindowQueryWithNulls(sql, expectedNulls);

        // range frame with default end
        sql = "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                "RANGE UNBOUNDED PRECEDING)";
        assertWindowQuery(sql, expected);
        assertWindowQueryWithNulls(sql, expectedNulls);

        // range frame with explicit end
        sql = "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
        assertWindowQuery(sql, expected);
        assertWindowQueryWithNulls(sql, expectedNulls);

        // rows frame with default end
        sql = "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                "ROWS UNBOUNDED PRECEDING)";
        assertWindowQuery(sql, expected);
        assertWindowQueryWithNulls(sql, expectedNulls);

        // rows frame with explicit end
        sql = "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
        assertWindowQuery(sql, expected);
        assertWindowQueryWithNulls(sql, expectedNulls);
    }

    @Test
    public void testSumRolling()
    {
        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS 2 PRECEDING)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", 3L)
                        .row(5, "F", 8L)
                        .row(6, "F", 14L)
                        .row(33, "F", 44L)
                        .row(1, "O", 1L)
                        .row(2, "O", 3L)
                        .row(4, "O", 7L)
                        .row(7, "O", 13L)
                        .row(32, "O", 43L)
                        .row(34, "O", 73L)
                        .build());

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", null)
                        .row(5, "F", null)
                        .row(6, "F", 3L)
                        .row(33, "F", 8L)
                        .row(1, "O", null)
                        .row(2, "O", null)
                        .row(4, "O", 1L)
                        .row(7, "O", 3L)
                        .row(32, "O", 7L)
                        .row(34, "O", 13L)
                        .build());

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", 47L)
                        .row(5, "F", 47L)
                        .row(6, "F", 47L)
                        .row(33, "F", 44L)
                        .row(1, "O", 14L)
                        .row(2, "O", 46L)
                        .row(4, "O", 80L)
                        .row(7, "O", 79L)
                        .row(32, "O", 77L)
                        .row(34, "O", 73L)
                        .build());

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", 14L)
                        .row(5, "F", 44L)
                        .row(6, "F", 39L)
                        .row(33, "F", 33L)
                        .row(1, "O", 7L)
                        .row(2, "O", 13L)
                        .row(4, "O", 43L)
                        .row(7, "O", 73L)
                        .row(32, "O", 66L)
                        .row(34, "O", 34L)
                        .build());

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN 2 FOLLOWING AND 4 FOLLOWING)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", 39L)
                        .row(5, "F", 33L)
                        .row(6, "F", null)
                        .row(33, "F", null)
                        .row(1, "O", 43L)
                        .row(2, "O", 73L)
                        .row(4, "O", 66L)
                        .row(7, "O", 34L)
                        .row(32, "O", null)
                        .row(34, "O", null)
                        .build());

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", 47L)
                        .row(5, "F", 44L)
                        .row(6, "F", 39L)
                        .row(33, "F", 33L)
                        .row(1, "O", 80L)
                        .row(2, "O", 79L)
                        .row(4, "O", 77L)
                        .row(7, "O", 73L)
                        .row(32, "O", 66L)
                        .row(34, "O", 34L)
                        .build());
    }

    @Test
    public void testSumRollingUnboundedPrecedingNPreceding()
    {
        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", 3L)
                        .row(5, "F", 8L)
                        .row(6, "F", 14L)
                        .row(33, "F", 47L)
                        .row(1, "O", 1L)
                        .row(2, "O", 3L)
                        .row(4, "O", 7L)
                        .row(7, "O", 14L)
                        .row(32, "O", 46L)
                        .row(34, "O", 80L)
                        .build());

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", null)
                        .row(5, "F", null)
                        .row(6, "F", 3L)
                        .row(33, "F", 8L)
                        .row(1, "O", null)
                        .row(2, "O", null)
                        .row(4, "O", 1L)
                        .row(7, "O", 3L)
                        .row(32, "O", 7L)
                        .row(34, "O", 14L)
                        .build());

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN UNBOUNDED PRECEDING AND 4 PRECEDING)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", null)
                        .row(5, "F", null)
                        .row(6, "F", null)
                        .row(33, "F", null)
                        .row(1, "O", null)
                        .row(2, "O", null)
                        .row(4, "O", null)
                        .row(7, "O", null)
                        .row(32, "O", 1L)
                        .row(34, "O", 3L)
                        .build());

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN UNBOUNDED PRECEDING AND 171 PRECEDING)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", null)
                        .row(5, "F", null)
                        .row(6, "F", null)
                        .row(33, "F", null)
                        .row(1, "O", null)
                        .row(2, "O", null)
                        .row(4, "O", null)
                        .row(7, "O", null)
                        .row(32, "O", null)
                        .row(34, "O", null)
                        .build());
    }

    @Test
    public void testSumRollingNFollowingUnboundedFollowing()
    {
        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN 0 FOLLOWING AND UNBOUNDED FOLLOWING)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", 47L)
                        .row(5, "F", 44L)
                        .row(6, "F", 39L)
                        .row(33, "F", 33L)
                        .row(1, "O", 80L)
                        .row(2, "O", 79L)
                        .row(4, "O", 77L)
                        .row(7, "O", 73L)
                        .row(32, "O", 66L)
                        .row(34, "O", 34L)
                        .build());

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN 3 FOLLOWING AND UNBOUNDED FOLLOWING)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", 33L)
                        .row(5, "F", null)
                        .row(6, "F", null)
                        .row(33, "F", null)
                        .row(1, "O", 73L)
                        .row(2, "O", 66L)
                        .row(4, "O", 34L)
                        .row(7, "O", null)
                        .row(32, "O", null)
                        .row(34, "O", null)
                        .build());

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN 4 FOLLOWING AND UNBOUNDED FOLLOWING)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", null)
                        .row(5, "F", null)
                        .row(6, "F", null)
                        .row(33, "F", null)
                        .row(1, "O", 66L)
                        .row(2, "O", 34L)
                        .row(4, "O", null)
                        .row(7, "O", null)
                        .row(32, "O", null)
                        .row(34, "O", null)
                        .build());

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN 2179 FOLLOWING AND UNBOUNDED FOLLOWING)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", null)
                        .row(5, "F", null)
                        .row(6, "F", null)
                        .row(33, "F", null)
                        .row(1, "O", null)
                        .row(2, "O", null)
                        .row(4, "O", null)
                        .row(7, "O", null)
                        .row(32, "O", null)
                        .row(34, "O", null)
                        .build());
    }

    @Test
    public void testSumCurrentRow()
            throws Exception
    {
        MaterializedResult expected = resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                .row(3, "F", 3L)
                .row(5, "F", 5L)
                .row(6, "F", 6L)
                .row(33, "F", 33L)
                .row(1, "O", 1L)
                .row(2, "O", 2L)
                .row(4, "O", 4L)
                .row(7, "O", 7L)
                .row(32, "O", 32L)
                .row(34, "O", 34L)
                .build();

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                "ROWS CURRENT ROW)", expected);

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                "ROWS BETWEEN CURRENT ROW AND CURRENT ROW)", expected);

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                "RANGE CURRENT ROW)", expected);

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                "RANGE BETWEEN CURRENT ROW AND CURRENT ROW)", expected);
    }

    @Test
    public void testSumEmptyWindow()
            throws Exception
    {
        MaterializedResult expected = resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                .row(3, "F", null)
                .row(5, "F", null)
                .row(6, "F", null)
                .row(33, "F", null)
                .row(1, "O", null)
                .row(2, "O", null)
                .row(4, "O", null)
                .row(7, "O", null)
                .row(32, "O", null)
                .row(34, "O", null)
                .build();

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                "ROWS BETWEEN 2 PRECEDING AND 3 PRECEDING)", expected);

        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                "ROWS BETWEEN 4 FOLLOWING AND 3 FOLLOWING)", expected);
    }

    @Test
    public void testSumUnordered()
    {
        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", 47L)
                        .row(5, "F", 47L)
                        .row(6, "F", 47L)
                        .row(33, "F", 47L)
                        .row(1, "O", 80L)
                        .row(2, "O", 80L)
                        .row(4, "O", 80L)
                        .row(7, "O", 80L)
                        .row(32, "O", 80L)
                        .row(34, "O", 80L)
                        .build());
        assertWindowQueryWithNulls("sum(orderkey) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", 8L)
                        .row(5L, "F", 8L)
                        .row(null, "F", 8L)
                        .row(null, "F", 8L)
                        .row(34L, "O", 34L)
                        .row(null, "O", 34L)
                        .row(1L, null, 8L)
                        .row(7L, null, 8L)
                        .row(null, null, 8L)
                        .row(null, null, 8L)
                        .build());
    }

    @Test
    public void testSumAllNulls()
    {
        assertWindowQueryWithNulls("sum(orderkey) OVER (PARTITION BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(1L, null, 1L)
                        .row(3L, "F", 3L)
                        .row(5L, "F", 5L)
                        .row(7L, null, 7L)
                        .row(34L, "O", 34L)
                        .row(null, "F", null)
                        .row(null, "F", null)
                        .row(null, "O", null)
                        .row(null, null, null)
                        .row(null, null, null)
                        .build());
    }
}
