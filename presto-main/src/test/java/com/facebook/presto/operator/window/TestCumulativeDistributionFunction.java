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
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;

public class TestCumulativeDistributionFunction
        extends AbstractTestWindowFunction
{
    @Test
    public void testCumulativeDistribution()
    {
        assertWindowQuery("cume_dist() OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, DOUBLE)
                        .row(3, "F", 0.25)
                        .row(5, "F", 0.5)
                        .row(6, "F", 0.75)
                        .row(33, "F", 1.0)
                        .row(1, "O", 1 / 6.0)
                        .row(2, "O", 2 / 6.0)
                        .row(4, "O", 3 / 6.0)
                        .row(7, "O", 4 / 6.0)
                        .row(32, "O", 5 / 6.0)
                        .row(34, "O", 1.0)
                        .build());
        assertWindowQueryWithNulls("cume_dist() OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(3L, "F", 0.25)
                        .row(5L, "F", 0.5)
                        .row(6L, "F", 0.75)
                        .row(null, "F", 1.0)
                        .row(34L, "O", 0.5)
                        .row(null, "O", 1.0)
                        .row(1L, null, 0.25)
                        .row(7L, null, 0.5)
                        .row(null, null, 1.0)
                        .row(null, null, 1.0)
                        .build());

        assertWindowQuery("cume_dist() OVER (ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, DOUBLE)
                        .row(1, "O", 0.1)
                        .row(2, "O", 0.2)
                        .row(3, "F", 0.3)
                        .row(4, "O", 0.4)
                        .row(5, "F", 0.5)
                        .row(6, "F", 0.6)
                        .row(7, "O", 0.7)
                        .row(32, "O", 0.8)
                        .row(33, "F", 0.9)
                        .row(34, "O", 1.0)
                        .build());
        assertWindowQueryWithNulls("cume_dist() OVER (ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(1L, null, 0.1)
                        .row(3L, "F", 0.2)
                        .row(5L, "F", 0.3)
                        .row(6L, "F", 0.4)
                        .row(7L, null, 0.5)
                        .row(34L, "O", 0.6)
                        .row(null, "F", 1.0)
                        .row(null, "O", 1.0)
                        .row(null, null, 1.0)
                        .row(null, null, 1.0)
                        .build());

        assertWindowQuery("cume_dist() OVER (ORDER BY orderstatus)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, DOUBLE)
                        .row(3, "F", 0.4)
                        .row(5, "F", 0.4)
                        .row(6, "F", 0.4)
                        .row(33, "F", 0.4)
                        .row(1, "O", 1.0)
                        .row(2, "O", 1.0)
                        .row(4, "O", 1.0)
                        .row(7, "O", 1.0)
                        .row(32, "O", 1.0)
                        .row(34, "O", 1.0)
                        .build());
        assertWindowQueryWithNulls("cume_dist() OVER (ORDER BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(3L, "F", 0.4)
                        .row(5L, "F", 0.4)
                        .row(6L, "F", 0.4)
                        .row(null, "F", 0.4)
                        .row(34L, "O", 0.6)
                        .row(null, "O", 0.6)
                        .row(1L, null, 1.0)
                        .row(7L, null, 1.0)
                        .row(null, null, 1.0)
                        .row(null, null, 1.0)
                        .build());

        assertWindowQuery("cume_dist() OVER (PARTITION BY orderkey)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, DOUBLE)
                        .row(1, "O", 1.0)
                        .row(2, "O", 1.0)
                        .row(3, "F", 1.0)
                        .row(4, "O", 1.0)
                        .row(5, "F", 1.0)
                        .row(6, "F", 1.0)
                        .row(7, "O", 1.0)
                        .row(32, "O", 1.0)
                        .row(33, "F", 1.0)
                        .row(34, "O", 1.0)
                        .build());
        assertWindowQueryWithNulls("cume_dist() OVER (PARTITION BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(1L, null, 1.0)
                        .row(3L, "F", 1.0)
                        .row(5L, "F", 1.0)
                        .row(6L, "F", 1.0)
                        .row(7L, null, 1.0)
                        .row(34L, "O", 1.0)
                        .row(null, "F", 1.0)
                        .row(null, "O", 1.0)
                        .row(null, null, 1.0)
                        .row(null, null, 1.0)
                        .build());
    }
}
