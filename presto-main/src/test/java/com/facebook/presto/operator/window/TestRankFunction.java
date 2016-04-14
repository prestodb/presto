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
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;

public class TestRankFunction
        extends AbstractTestWindowFunction
{
    @Test
    public void testRank()
    {
        assertWindowQuery("rank() OVER (ORDER BY orderstatus)",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                        .row(3, "F", 1L)
                        .row(5, "F", 1L)
                        .row(6, "F", 1L)
                        .row(33, "F", 1L)
                        .row(1, "O", 5L)
                        .row(2, "O", 5L)
                        .row(4, "O", 5L)
                        .row(7, "O", 5L)
                        .row(32, "O", 5L)
                        .row(34, "O", 5L)
                        .build());
        assertWindowQueryWithNulls("rank() OVER (ORDER BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(3L, "F", 1L)
                        .row(5L, "F", 1L)
                        .row(null, "F", 1L)
                        .row(null, "F", 1L)
                        .row(34L, "O", 5L)
                        .row(null, "O", 5L)
                        .row(1L, null, 7L)
                        .row(7L, null, 7L)
                        .row(null, null, 7L)
                        .row(null, null, 7L)
                        .build());
    }
}
