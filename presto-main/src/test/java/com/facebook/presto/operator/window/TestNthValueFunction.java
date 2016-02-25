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

public class TestNthValueFunction
        extends AbstractTestWindowFunction
{
    @Test
    public void testNthValueUnbounded()
    {
        // constant offset
        assertUnboundedWindowQuery("nth_value(orderkey, 2) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3L, "F", 5L)
                        .row(5L, "F", 5L)
                        .row(6L, "F", 5L)
                        .row(33L, "F", 5L)
                        .row(1L, "O", 2L)
                        .row(2L, "O", 2L)
                        .row(4L, "O", 2L)
                        .row(7L, "O", 2L)
                        .row(32L, "O", 2L)
                        .row(34L, "O", 2L)
                        .build());
        assertUnboundedWindowQueryWithNulls("nth_value(orderkey, 2) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3L, "F", 5L)
                        .row(5L, "F", 5L)
                        .row(null, "F", 5L)
                        .row(null, "F", 5L)
                        .row(34L, "O", null)
                        .row(null, "O", null)
                        .row(1L, null, 7L)
                        .row(7L, null, 7L)
                        .row(null, null, 7L)
                        .row(null, null, 7L)
                        .build());

        // variable offset
        assertUnboundedWindowQuery("nth_value(orderkey, orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3L, "F", 6L)
                        .row(5L, "F", null)
                        .row(6L, "F", null)
                        .row(33L, "F", null)
                        .row(1L, "O", 1L)
                        .row(2L, "O", 2L)
                        .row(4L, "O", 7L)
                        .row(7L, "O", null)
                        .row(32L, "O", null)
                        .row(34L, "O", null)
                        .build());
        assertUnboundedWindowQueryWithNulls(
                "nth_value(orderkey, orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey DESC NULLS FIRST)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3L, "F", 5L)
                        .row(5L, "F", null)
                        .row(null, "F", null)
                        .row(null, "F", null)
                        .row(34L, "O", null)
                        .row(null, "O", null)
                        .row(1L, null, null)
                        .row(7L, null, null)
                        .row(null, null, null)
                        .row(null, null, null)
                        .build());

        // null offset
        assertUnboundedWindowQuery("nth_value(orderkey, null) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3L, "F", null)
                        .row(5L, "F", null)
                        .row(6L, "F", null)
                        .row(33L, "F", null)
                        .row(1L, "O", null)
                        .row(2L, "O", null)
                        .row(4L, "O", null)
                        .row(7L, "O", null)
                        .row(32L, "O", null)
                        .row(34L, "O", null)
                        .build());

        // huge offset (larger than an int)
        assertUnboundedWindowQuery("nth_value(orderkey, 8 * 1000 * 1000 * 1000) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3L, "F", null)
                        .row(5L, "F", null)
                        .row(6L, "F", null)
                        .row(33L, "F", null)
                        .row(1L, "O", null)
                        .row(2L, "O", null)
                        .row(4L, "O", null)
                        .row(7L, "O", null)
                        .row(32L, "O", null)
                        .row(34L, "O", null)
                        .build());
    }

    @Test
    public void testNthValueBounded()
    {
        assertWindowQuery("nth_value(orderkey, 4) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", null)
                        .row(5L, "F", 33L)
                        .row(6L, "F", 33L)
                        .row(33L, "F", null)
                        .row(1L, "O", null)
                        .row(2L, "O", 7L)
                        .row(4L, "O", 7L)
                        .row(7L, "O", 32L)
                        .row(32L, "O", 34L)
                        .row(34L, "O", null)
                        .build());
        assertWindowQueryWithNulls("nth_value(orderkey, 4) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", null)
                        .row(5L, "F", null)
                        .row(null, "F", null)
                        .row(null, "F", null)
                        .row(34L, "O", null)
                        .row(null, "O", null)
                        .row(1L, null, null)
                        .row(7L, null, null)
                        .row(null, null, null)
                        .row(null, null, null)
                        .build());

        // Timestamp
        assertWindowQuery("date_format(nth_value(cast(orderdate as TIMESTAMP), 2) OVER (PARTITION BY orderstatus ORDER BY orderkey), '%Y-%m-%d')",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3L, "F", null)
                        .row(5L, "F", "1994-07-30")
                        .row(6L, "F", "1994-07-30")
                        .row(33L, "F", "1994-07-30")
                        .row(1L, "O", null)
                        .row(2L, "O", "1996-12-01")
                        .row(4L, "O", "1996-12-01")
                        .row(7L, "O", "1996-12-01")
                        .row(32L, "O", "1996-12-01")
                        .row(34L, "O", "1996-12-01")
                        .build());
    }
}
