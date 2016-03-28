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
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;

public class TestPercentRankFunction
        extends AbstractTestWindowFunction
{
    @Test
    public void testPercentRank()
    {
        assertWindowQuery("percent_rank() OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(3L, "F", 0.0)
                        .row(5L, "F", 1 / 3.0)
                        .row(6L, "F", 2 / 3.0)
                        .row(33L, "F", 1.0)
                        .row(1L, "O", 0.0)
                        .row(2L, "O", 0.2)
                        .row(4L, "O", 0.4)
                        .row(7L, "O", 0.6)
                        .row(32L, "O", 0.8)
                        .row(34L, "O", 1.0)
                        .build());
        assertWindowQueryWithNulls("percent_rank() OVER (PARTITION BY orderstatus ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", 0.0)
                        .row(5L, "F", 1 / 3.0)
                        .row(null, "F", 2 / 3.0)
                        .row(null, "F", 2 / 3.0)
                        .row(34L, "O", 0.0)
                        .row(null, "O", 1.0)
                        .row(1L, null, 0.0)
                        .row(7L, null, 1 / 3.0)
                        .row(null, null, 2 / 3.0)
                        .row(null, null, 2 / 3.0)
                        .build());

        assertWindowQuery("percent_rank() OVER (ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(1L, "O", 0.0)
                        .row(2L, "O", 1 / 9.0)
                        .row(3L, "F", 2 / 9.0)
                        .row(4L, "O", 3 / 9.0)
                        .row(5L, "F", 4 / 9.0)
                        .row(6L, "F", 5 / 9.0)
                        .row(7L, "O", 6 / 9.0)
                        .row(32L, "O", 7 / 9.0)
                        .row(33L, "F", 8 / 9.0)
                        .row(34L, "O", 1.0)
                        .build());
        assertWindowQueryWithNulls("percent_rank() OVER (ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(1L, null, 0.0)
                        .row(3L, "F", 1 / 9.0)
                        .row(5L, "F", 2 / 9.0)
                        .row(7L, null, 3 / 9.0)
                        .row(34L, "O", 4 / 9.0)
                        .row(null, "F", 5 / 9.0)
                        .row(null, "F", 5 / 9.0)
                        .row(null, "O", 5 / 9.0)
                        .row(null, null, 5 / 9.0)
                        .row(null, null, 5 / 9.0)
                        .build());

        assertWindowQuery("percent_rank() OVER (ORDER BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(3L, "F", 0.0)
                        .row(5L, "F", 0.0)
                        .row(6L, "F", 0.0)
                        .row(33L, "F", 0.0)
                        .row(1L, "O", 4 / 9.0)
                        .row(2L, "O", 4 / 9.0)
                        .row(4L, "O", 4 / 9.0)
                        .row(7L, "O", 4 / 9.0)
                        .row(32L, "O", 4 / 9.0)
                        .row(34L, "O", 4 / 9.0)
                        .build());
        assertWindowQueryWithNulls("percent_rank() OVER (ORDER BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", 0.0)
                        .row(5L, "F", 0.0)
                        .row(null, "F", 0.0)
                        .row(null, "F", 0.0)
                        .row(34L, "O", 4 / 9.0)
                        .row(null, "O", 4 / 9.0)
                        .row(1L, null, 6 / 9.0)
                        .row(7L, null, 6 / 9.0)
                        .row(null, null, 6 / 9.0)
                        .row(null, null, 6 / 9.0)
                        .build());

        assertWindowQuery("percent_rank() OVER (PARTITION BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, DOUBLE)
                        .row(1L, "O", 0.0)
                        .row(2L, "O", 0.0)
                        .row(3L, "F", 0.0)
                        .row(4L, "O", 0.0)
                        .row(5L, "F", 0.0)
                        .row(6L, "F", 0.0)
                        .row(7L, "O", 0.0)
                        .row(32L, "O", 0.0)
                        .row(33L, "F", 0.0)
                        .row(34L, "O", 0.0)
                        .build());
        assertWindowQueryWithNulls("percent_rank() OVER (PARTITION BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(1L, null, 0.0)
                        .row(3L, "F", 0.0)
                        .row(5L, "F", 0.0)
                        .row(7L, null, 0.0)
                        .row(34L, "O", 0.0)
                        .row(null, "F", 0.0)
                        .row(null, "F", 0.0)
                        .row(null, "O", 0.0)
                        .row(null, null, 0.0)
                        .row(null, null, 0.0)
                        .build());
    }
}
