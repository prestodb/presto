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

public class TestAggregateWindowFunction
        extends AbstractTestWindowFunction
{
    @Test
    public void testCountRowsOrdered()
    {
        assertWindowQuery("count(*) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 1)
                        .row(5, "F", 2)
                        .row(6, "F", 3)
                        .row(33, "F", 4)
                        .row(1, "O", 1)
                        .row(2, "O", 2)
                        .row(4, "O", 3)
                        .row(7, "O", 4)
                        .row(32, "O", 5)
                        .row(34, "O", 6)
                        .build());
        assertWindowQueryWithNulls("count(*) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 1)
                        .row(5, "F", 2)
                        .row(null, "F", 4)
                        .row(null, "F", 4)
                        .row(34, "O", 1)
                        .row(null, "O", 2)
                        .row(1, null, 1)
                        .row(7, null, 2)
                        .row(null, null, 4)
                        .row(null, null, 4)
                        .build());
    }

    @Test
    public void testCountRowsUnordered()
    {
        assertWindowQuery("count(*) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 4)
                        .row(5, "F", 4)
                        .row(6, "F", 4)
                        .row(33, "F", 4)
                        .row(1, "O", 6)
                        .row(2, "O", 6)
                        .row(4, "O", 6)
                        .row(7, "O", 6)
                        .row(32, "O", 6)
                        .row(34, "O", 6)
                        .build());
        assertWindowQueryWithNulls("count(*) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 4)
                        .row(5, "F", 4)
                        .row(null, "F", 4)
                        .row(null, "F", 4)
                        .row(34, "O", 2)
                        .row(null, "O", 2)
                        .row(1, null, 4)
                        .row(7, null, 4)
                        .row(null, null, 4)
                        .row(null, null, 4)
                        .build());
    }

    @Test
    public void testCountValuesOrdered()
    {
        assertWindowQuery("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 1)
                        .row(5, "F", 2)
                        .row(6, "F", 3)
                        .row(33, "F", 4)
                        .row(1, "O", 1)
                        .row(2, "O", 2)
                        .row(4, "O", 3)
                        .row(7, "O", 4)
                        .row(32, "O", 5)
                        .row(34, "O", 6)
                        .build());
        assertWindowQueryWithNulls("count(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 1)
                        .row(5, "F", 2)
                        .row(null, "F", 2)
                        .row(null, "F", 2)
                        .row(34, "O", 1)
                        .row(null, "O", 1)
                        .row(1, null, 1)
                        .row(7, null, 2)
                        .row(null, null, 2)
                        .row(null, null, 2)
                        .build());
    }

    @Test
    public void testCountValuesUnordered()
    {
        assertWindowQuery("count(orderkey) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 4)
                        .row(5, "F", 4)
                        .row(6, "F", 4)
                        .row(33, "F", 4)
                        .row(1, "O", 6)
                        .row(2, "O", 6)
                        .row(4, "O", 6)
                        .row(7, "O", 6)
                        .row(32, "O", 6)
                        .row(34, "O", 6)
                        .build());
        assertWindowQueryWithNulls("count(orderkey) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 2)
                        .row(5, "F", 2)
                        .row(null, "F", 2)
                        .row(null, "F", 2)
                        .row(34, "O", 1)
                        .row(null, "O", 1)
                        .row(1, null, 2)
                        .row(7, null, 2)
                        .row(null, null, 2)
                        .row(null, null, 2)
                        .build());
    }

    @Test
    public void testSumOrdered()
    {
        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 3)
                        .row(5, "F", 8)
                        .row(6, "F", 14)
                        .row(33, "F", 47)
                        .row(1, "O", 1)
                        .row(2, "O", 3)
                        .row(4, "O", 7)
                        .row(7, "O", 14)
                        .row(32, "O", 46)
                        .row(34, "O", 80)
                        .build());
        assertWindowQueryWithNulls("sum(orderkey) OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 3)
                        .row(5, "F", 8)
                        .row(null, "F", 8)
                        .row(null, "F", 8)
                        .row(34, "O", 34)
                        .row(null, "O", 34)
                        .row(1, null, 1)
                        .row(7, null, 8)
                        .row(null, null, 8)
                        .row(null, null, 8)
                        .build());
    }

    @Test
    public void testSumUnordered()
    {
        assertWindowQuery("sum(orderkey) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 47)
                        .row(5, "F", 47)
                        .row(6, "F", 47)
                        .row(33, "F", 47)
                        .row(1, "O", 80)
                        .row(2, "O", 80)
                        .row(4, "O", 80)
                        .row(7, "O", 80)
                        .row(32, "O", 80)
                        .row(34, "O", 80)
                        .build());
        assertWindowQueryWithNulls("sum(orderkey) OVER (PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3, "F", 8)
                        .row(5, "F", 8)
                        .row(null, "F", 8)
                        .row(null, "F", 8)
                        .row(34, "O", 34)
                        .row(null, "O", 34)
                        .row(1, null, 8)
                        .row(7, null, 8)
                        .row(null, null, 8)
                        .row(null, null, 8)
                        .build());
    }

    @Test
    public void testSumAllNulls()
    {
        assertWindowQueryWithNulls("sum(orderkey) OVER (PARTITION BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(1, null, 1)
                        .row(3, "F", 3)
                        .row(5, "F", 5)
                        .row(7, null, 7)
                        .row(34, "O", 34)
                        .row(null, "F", null)
                        .row(null, "F", null)
                        .row(null, "O", null)
                        .row(null, null, null)
                        .row(null, null, null)
                        .build());
    }
}
