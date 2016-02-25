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

public class TestLagFunction
        extends AbstractTestWindowFunction
{
    @Test
    public void testLagFunction()
    {
        assertWindowQuery("lag(orderdate) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3L, "F", null)
                        .row(5L, "F", "1993-10-14")
                        .row(6L, "F", "1994-07-30")
                        .row(33L, "F", "1992-02-21")
                        .row(1L, "O", null)
                        .row(2L, "O", "1996-01-02")
                        .row(4L, "O", "1996-12-01")
                        .row(7L, "O", "1995-10-11")
                        .row(32L, "O", "1996-01-10")
                        .row(34L, "O", "1995-07-16")
                        .build());
        assertWindowQueryWithNulls("lag(orderdate) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3L, "F", null)
                        .row(5L, "F", "1993-10-14")
                        .row(null, "F", null)
                        .row(null, "F", "1993-10-27")
                        .row(34L, "O", null)
                        .row(null, "O", "1998-07-21")
                        .row(1L, null, null)
                        .row(7L, null, null)
                        .row(null, null, "1996-01-10")
                        .row(null, null, null)
                        .build());

        assertWindowQuery("lag(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", null)
                        .row(5L, "F", 3L)
                        .row(6L, "F", 5L)
                        .row(33L, "F", 6L)
                        .row(1L, "O", null)
                        .row(2L, "O", 1L)
                        .row(4L, "O", 2L)
                        .row(7L, "O", 4L)
                        .row(32L, "O", 7L)
                        .row(34L, "O", 32L)
                        .build());
        assertWindowQueryWithNulls("lag(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", null)
                        .row(5L, "F", 3L)
                        .row(null, "F", 5L)
                        .row(null, "F", null)
                        .row(34L, "O", null)
                        .row(null, "O", 34L)
                        .row(1L, null, null)
                        .row(7L, null, 1L)
                        .row(null, null, 7L)
                        .row(null, null, null)
                        .build());

        assertWindowQuery("lag(orderdate, 2, '1977-01-01') OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3L, "F", "1977-01-01")
                        .row(5L, "F", "1977-01-01")
                        .row(6L, "F", "1993-10-14")
                        .row(33L, "F", "1994-07-30")
                        .row(1L, "O", "1977-01-01")
                        .row(2L, "O", "1977-01-01")
                        .row(4L, "O", "1996-01-02")
                        .row(7L, "O", "1996-12-01")
                        .row(32L, "O", "1995-10-11")
                        .row(34L, "O", "1996-01-10")
                        .build());
        assertWindowQueryWithNulls("lag(orderdate, 2, '1977-01-01') OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3L, "F", "1977-01-01")
                        .row(5L, "F", "1977-01-01")
                        .row(null, "F", "1993-10-14")
                        .row(null, "F", null)
                        .row(34L, "O", "1977-01-01")
                        .row(null, "O", "1977-01-01")
                        .row(1L, null, "1977-01-01")
                        .row(7L, null, "1977-01-01")
                        .row(null, null, null)
                        .row(null, null, "1996-01-10")
                        .build());

        assertWindowQuery("lag(orderkey, 2, -1) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", -1L)
                        .row(5L, "F", -1L)
                        .row(6L, "F", 3L)
                        .row(33L, "F", 5L)
                        .row(1L, "O", -1L)
                        .row(2L, "O", -1L)
                        .row(4L, "O", 1L)
                        .row(7L, "O", 2L)
                        .row(32L, "O", 4L)
                        .row(34L, "O", 7L)
                        .build());
        assertWindowQueryWithNulls("lag(orderkey, 2, -1) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", -1L)
                        .row(5L, "F", -1L)
                        .row(null, "F", 3L)
                        .row(null, "F", 5L)
                        .row(34L, "O", -1L)
                        .row(null, "O", -1L)
                        .row(1L, null, -1L)
                        .row(7L, null, -1L)
                        .row(null, null, 1L)
                        .row(null, null, 7L)
                        .build());

        assertWindowQuery("lag(orderkey, 8 * 1000 * 1000 * 1000) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
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

        assertWindowQuery("lag(orderkey, null, -1) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
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

        assertWindowQuery("lag(orderkey, 0) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", 3L)
                        .row(5L, "F", 5L)
                        .row(6L, "F", 6L)
                        .row(33L, "F", 33L)
                        .row(1L, "O", 1L)
                        .row(2L, "O", 2L)
                        .row(4L, "O", 4L)
                        .row(7L, "O", 7L)
                        .row(32L, "O", 32L)
                        .row(34L, "O", 34L)
                        .build());

        assertWindowQuery("date_format(lag(cast(orderdate as TIMESTAMP), 0) OVER (PARTITION BY orderstatus ORDER BY orderkey), '%Y-%m-%d')",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3L, "F", "1993-10-14")
                        .row(5L, "F", "1994-07-30")
                        .row(6L, "F", "1992-02-21")
                        .row(33L, "F", "1993-10-27")
                        .row(1L, "O", "1996-01-02")
                        .row(2L, "O", "1996-12-01")
                        .row(4L, "O", "1995-10-11")
                        .row(7L, "O", "1996-01-10")
                        .row(32L, "O", "1995-07-16")
                        .row(34L, "O", "1998-07-21")
                        .build());
    }
}
