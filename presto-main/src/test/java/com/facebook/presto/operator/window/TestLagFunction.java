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
                        .row(3, "F", null)
                        .row(5, "F", "1993-10-14")
                        .row(6, "F", "1994-07-30")
                        .row(33, "F", "1992-02-21")
                        .row(1, "O", null)
                        .row(2, "O", "1996-01-02")
                        .row(4, "O", "1996-12-01")
                        .row(7, "O", "1995-10-11")
                        .row(32, "O", "1996-01-10")
                        .row(34, "O", "1995-07-16")
                        .build());
        assertWindowQueryWithNulls("lag(orderdate) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", null)
                        .row(5, "F", "1993-10-14")
                        .row(null, "F", null)
                        .row(null, "F", "1993-10-27")
                        .row(34, "O", null)
                        .row(null, "O", "1998-07-21")
                        .row(1, null, null)
                        .row(7, null, null)
                        .row(null, null, "1996-01-10")
                        .row(null, null, null)
                        .build());

        assertWindowQuery("lag(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", null)
                        .row(5, "F", 3)
                        .row(6, "F", 5)
                        .row(33, "F", 6)
                        .row(1, "O", null)
                        .row(2, "O", 1)
                        .row(4, "O", 2)
                        .row(7, "O", 4)
                        .row(32, "O", 7)
                        .row(34, "O", 32)
                        .build());
        assertWindowQueryWithNulls("lag(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", null)
                        .row(5, "F", 3)
                        .row(null, "F", 5)
                        .row(null, "F", null)
                        .row(34, "O", null)
                        .row(null, "O", 34)
                        .row(1, null, null)
                        .row(7, null, 1)
                        .row(null, null, 7)
                        .row(null, null, null)
                        .build());

        assertWindowQuery("lag(orderdate, 2, '1977-01-01') OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", "1977-01-01")
                        .row(5, "F", "1977-01-01")
                        .row(6, "F", "1993-10-14")
                        .row(33, "F", "1994-07-30")
                        .row(1, "O", "1977-01-01")
                        .row(2, "O", "1977-01-01")
                        .row(4, "O", "1996-01-02")
                        .row(7, "O", "1996-12-01")
                        .row(32, "O", "1995-10-11")
                        .row(34, "O", "1996-01-10")
                        .build());
        assertWindowQueryWithNulls("lag(orderdate, 2, '1977-01-01') OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", "1977-01-01")
                        .row(5, "F", "1977-01-01")
                        .row(null, "F", "1993-10-14")
                        .row(null, "F", null)
                        .row(34, "O", "1977-01-01")
                        .row(null, "O", "1977-01-01")
                        .row(1, null, "1977-01-01")
                        .row(7, null, "1977-01-01")
                        .row(null, null, null)
                        .row(null, null, "1996-01-10")
                        .build());

        assertWindowQuery("lag(orderkey, 2, -1) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", -1)
                        .row(5, "F", -1)
                        .row(6, "F", 3)
                        .row(33, "F", 5)
                        .row(1, "O", -1)
                        .row(2, "O", -1)
                        .row(4, "O", 1)
                        .row(7, "O", 2)
                        .row(32, "O", 4)
                        .row(34, "O", 7)
                        .build());
        assertWindowQueryWithNulls("lag(orderkey, 2, -1) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", -1)
                        .row(5, "F", -1)
                        .row(null, "F", 3)
                        .row(null, "F", 5)
                        .row(34, "O", -1)
                        .row(null, "O", -1)
                        .row(1, null, -1)
                        .row(7, null, -1)
                        .row(null, null, 1)
                        .row(null, null, 7)
                        .build());

        assertWindowQuery("lag(orderkey, 8 * 1000 * 1000 * 1000) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
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

        assertWindowQuery("lag(orderkey, null, -1) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
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

        assertWindowQuery("lag(orderkey, 0) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 3)
                        .row(5, "F", 5)
                        .row(6, "F", 6)
                        .row(33, "F", 33)
                        .row(1, "O", 1)
                        .row(2, "O", 2)
                        .row(4, "O", 4)
                        .row(7, "O", 7)
                        .row(32, "O", 32)
                        .row(34, "O", 34)
                        .build());

        assertWindowQuery("date_format(lag(cast(orderdate as TIMESTAMP), 0) OVER (PARTITION BY orderstatus ORDER BY orderkey), '%Y-%m-%d')",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", "1993-10-14")
                        .row(5, "F", "1994-07-30")
                        .row(6, "F", "1992-02-21")
                        .row(33, "F", "1993-10-27")
                        .row(1, "O", "1996-01-02")
                        .row(2, "O", "1996-12-01")
                        .row(4, "O", "1995-10-11")
                        .row(7, "O", "1996-01-10")
                        .row(32, "O", "1995-07-16")
                        .row(34, "O", "1998-07-21")
                        .build());
    }
}
