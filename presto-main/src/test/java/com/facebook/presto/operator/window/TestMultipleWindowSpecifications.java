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

import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;

public class TestMultipleWindowSpecifications
        extends AbstractTestWindowFunction
{
    @Test
    public void testIdenticalWindowSpecifications()
    {
        assertWindowQuery("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey), " +
                        "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT, BIGINT)
                        .row(3, "F", 1L, 3L)
                        .row(5, "F", 2L, 8L)
                        .row(6, "F", 3L, 14L)
                        .row(33, "F", 4L, 47L)
                        .row(1, "O", 1L, 1L)
                        .row(2, "O", 2L, 3L)
                        .row(4, "O", 3L, 7L)
                        .row(7, "O", 4L, 14L)
                        .row(32, "O", 5L, 46L)
                        .row(34, "O", 6L, 80L)
                        .build());

        assertWindowQueryWithNulls("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey), " +
                        "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT, BIGINT)
                        .row(3L, "F", 1L, 3L)
                        .row(5L, "F", 2L, 8L)
                        .row(null, "F", 2L, 8L)
                        .row(null, "F", 2L, 8L)
                        .row(34L, "O", 1L, 34L)
                        .row(null, "O", 1L, 34L)
                        .row(1L, null, 1L, 1L)
                        .row(7L, null, 2L, 8L)
                        .row(null, null, 2L, 8L)
                        .row(null, null, 2L, 8L)
                        .build());
    }

    @Test
    public void testIntersectingWindowSpecifications()
    {
        // Intersection previous to current row
        assertWindowQuery("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING), " +
                        "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT, BIGINT)
                        .row(3, "F", 0L, 3L)
                        .row(5, "F", 0L, 8L)
                        .row(6, "F", 1L, 14L)
                        .row(33, "F", 2L, 44L)
                        .row(1, "O", 0L, 1L)
                        .row(2, "O", 0L, 3L)
                        .row(4, "O", 1L, 7L)
                        .row(7, "O", 2L, 13L)
                        .row(32, "O", 2L, 43L)
                        .row(34, "O", 2L, 73L)
                        .build());

        assertWindowQueryWithNulls("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING), " +
                        "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT, BIGINT)
                        .row(3L, "F", 0L, 3L)
                        .row(5L, "F", 0L, 8L)
                        .row(null, "F", 1L, 8L)
                        .row(null, "F", 2L, 5L)
                        .row(34L, "O", 0L, 34L)
                        .row(null, "O", 0L, 34L)
                        .row(1L, null, 0L, 1L)
                        .row(7L, null, 0L, 8L)
                        .row(null, null, 1L, 8L)
                        .row(null, null, 2L, 7L)
                        .build());

        // Intersection at current row
        assertWindowQuery("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), " +
                        "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT, BIGINT)
                        .row(3, "F", 1L, 14L)
                        .row(5, "F", 2L, 44L)
                        .row(6, "F", 2L, 39L)
                        .row(33, "F", 2L, 33L)
                        .row(1, "O", 1L, 7L)
                        .row(2, "O", 2L, 13L)
                        .row(4, "O", 2L, 43L)
                        .row(7, "O", 2L, 73L)
                        .row(32, "O", 2L, 66L)
                        .row(34, "O", 2L, 34L)
                        .build());

        assertWindowQueryWithNulls("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), " +
                        "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT, BIGINT)
                        .row(3L, "F", 1L, 8L)
                        .row(5L, "F", 2L, 5L)
                        .row(null, "F", 1L, null)
                        .row(null, "F", 0L, null)
                        .row(34L, "O", 1L, 34L)
                        .row(null, "O", 1L, null)
                        .row(1L, null, 1L, 8L)
                        .row(7L, null, 2L, 7L)
                        .row(null, null, 1L, null)
                        .row(null, null, 0L, null)
                        .build());

        // Intersection following current row
        assertWindowQuery("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING), " +
                        "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT, BIGINT)
                        .row(3, "F", 2L, 11L)
                        .row(5, "F", 2L, 39L)
                        .row(6, "F", 2L, 33L)
                        .row(33, "F", 1L, null)
                        .row(1, "O", 2L, 6L)
                        .row(2, "O", 2L, 11L)
                        .row(4, "O", 2L, 39L)
                        .row(7, "O", 2L, 66L)
                        .row(32, "O", 2L, 34L)
                        .row(34, "O", 1L, null)
                        .build());

        assertWindowQueryWithNulls("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING), " +
                        "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT, BIGINT)
                        .row(3L, "F", 2L, 5L)
                        .row(5L, "F", 1L, null)
                        .row(null, "F", 0L, null)
                        .row(null, "F", 0L, null)
                        .row(34L, "O", 1L, null)
                        .row(null, "O", 0L, null)
                        .row(1L, null, 2L, 7L)
                        .row(7L, null, 1L, null)
                        .row(null, null, 0L, null)
                        .row(null, null, 0L, null)
                        .build());
    }

    @Test
    public void testDisjointWindowSpecifications()
    {
        assertWindowQuery("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING), " +
                        "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT, BIGINT)
                        .row(3, "F", 0L, 3L)
                        .row(5, "F", 0L, 8L)
                        .row(6, "F", 1L, 11L)
                        .row(33, "F", 2L, 39L)
                        .row(1, "O", 0L, 1L)
                        .row(2, "O", 0L, 3L)
                        .row(4, "O", 1L, 6L)
                        .row(7, "O", 2L, 11L)
                        .row(32, "O", 2L, 39L)
                        .row(34, "O", 2L, 66L)
                        .build());

        assertWindowQueryWithNulls("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING), " +
                        "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT, BIGINT)
                        .row(3L, "F", 0L, 3L)
                        .row(5L, "F", 0L, 8L)
                        .row(null, "F", 1L, 5L)
                        .row(null, "F", 2L, null)
                        .row(34L, "O", 0L, 34L)
                        .row(null, "O", 0L, 34L)
                        .row(1L, null, 0L, 1L)
                        .row(7L, null, 0L, 8L)
                        .row(null, null, 1L, 7L)
                        .row(null, null, 2L, null)
                        .build());

        assertWindowQuery("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING), " +
                        "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT, BIGINT)
                        .row(3, "F", 0L, 44L)
                        .row(5, "F", 0L, 39L)
                        .row(6, "F", 1L, 33L)
                        .row(33, "F", 2L, null)
                        .row(1, "O", 0L, 79L)
                        .row(2, "O", 0L, 77L)
                        .row(4, "O", 1L, 73L)
                        .row(7, "O", 2L, 66L)
                        .row(32, "O", 2L, 34L)
                        .row(34, "O", 2L, null)
                        .build());

        assertWindowQueryWithNulls("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING), " +
                        "sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT, BIGINT)
                        .row(3L, "F", 0L, 5L)
                        .row(5L, "F", 0L, null)
                        .row(null, "F", 1L, null)
                        .row(null, "F", 2L, null)
                        .row(34L, "O", 0L, null)
                        .row(null, "O", 0L, null)
                        .row(1L, null, 0L, 7L)
                        .row(7L, null, 0L, null)
                        .row(null, null, 1L, null)
                        .row(null, null, 2L, null)
                        .build());
    }
}
