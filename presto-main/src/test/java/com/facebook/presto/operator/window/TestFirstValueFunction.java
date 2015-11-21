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

public class TestFirstValueFunction
        extends AbstractTestWindowFunction
{
    @Test
    public void testFirstValueUnbounded()
    {
        assertWindowQuery("first_value(orderdate) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", "1993-10-14")
                        .row(5, "F", "1993-10-14")
                        .row(6, "F", "1993-10-14")
                        .row(33, "F", "1993-10-14")
                        .row(1, "O", "1996-01-02")
                        .row(2, "O", "1996-01-02")
                        .row(4, "O", "1996-01-02")
                        .row(7, "O", "1996-01-02")
                        .row(32, "O", "1996-01-02")
                        .row(34, "O", "1996-01-02")
                        .build());
        assertWindowQueryWithNulls("first_value(orderdate) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", "1993-10-14")
                        .row(5, "F", "1993-10-14")
                        .row(null, "F", "1993-10-14")
                        .row(null, "F", "1993-10-14")
                        .row(34, "O", "1998-07-21")
                        .row(null, "O", "1998-07-21")
                        .row(1, null, null)
                        .row(7, null, null)
                        .row(null, null, null)
                        .row(null, null, null)
                        .build());

        assertWindowQuery("first_value(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 3)
                        .row(5, "F", 3)
                        .row(6, "F", 3)
                        .row(33, "F", 3)
                        .row(1, "O", 1)
                        .row(2, "O", 1)
                        .row(4, "O", 1)
                        .row(7, "O", 1)
                        .row(32, "O", 1)
                        .row(34, "O", 1)
                        .build());
        assertWindowQueryWithNulls("first_value(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 3)
                        .row(5, "F", 3)
                        .row(null, "F", 3)
                        .row(null, "F", 3)
                        .row(34, "O", 34)
                        .row(null, "O", 34)
                        .row(1, null, 1)
                        .row(7, null, 1)
                        .row(null, null, 1)
                        .row(null, null, 1)
                        .build());

        // Timestamp
        assertWindowQuery("date_format(first_value(cast(orderdate as TIMESTAMP)) OVER (PARTITION BY orderstatus ORDER BY orderkey), '%Y-%m-%d')",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, VARCHAR)
                        .row(3, "F", "1993-10-14")
                        .row(5, "F", "1993-10-14")
                        .row(6, "F", "1993-10-14")
                        .row(33, "F", "1993-10-14")
                        .row(1, "O", "1996-01-02")
                        .row(2, "O", "1996-01-02")
                        .row(4, "O", "1996-01-02")
                        .row(7, "O", "1996-01-02")
                        .row(32, "O", "1996-01-02")
                        .row(34, "O", "1996-01-02")
                        .build());
    }

    @Test
    public void testFirstValueBounded()
    {
        assertWindowQuery("first_value(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 3)
                        .row(5, "F", 3)
                        .row(6, "F", 3)
                        .row(33, "F", 5)
                        .row(1, "O", 1)
                        .row(2, "O", 1)
                        .row(4, "O", 1)
                        .row(7, "O", 2)
                        .row(32, "O", 4)
                        .row(34, "O", 7)
                        .build());
        assertWindowQueryWithNulls("first_value(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey " +
                        "ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 3)
                        .row(5, "F", 3)
                        .row(null, "F", 3)
                        .row(null, "F", 5)
                        .row(34, "O", 34)
                        .row(null, "O", 34)
                        .row(1, null, 1)
                        .row(7, null, 1)
                        .row(null, null, 1)
                        .row(null, null, 7)
                        .build());
    }
}
