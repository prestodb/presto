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
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;

public class TestRowNumberFunction
        extends AbstractTestWindowFunction
{
    @Test
    public void testRowNumber()
    {
        MaterializedResult expected = resultBuilder(TEST_SESSION, INTEGER, VARCHAR, BIGINT)
                .row(1, "O", 1L)
                .row(2, "O", 2L)
                .row(3, "F", 3L)
                .row(4, "O", 4L)
                .row(5, "F", 5L)
                .row(6, "F", 6L)
                .row(7, "O", 7L)
                .row(32, "O", 8L)
                .row(33, "F", 9L)
                .row(34, "O", 10L)
                .build();
        MaterializedResult expectedWithNulls = resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                .row(1L, null, 1L)
                .row(3L, "F", 2L)
                .row(5L, "F", 3L)
                .row(7L, null, 4L)
                .row(34L, "O", 5L)
                .row(null, "F", 6L)
                .row(null, "F", 7L)
                .row(null, "O", 8L)
                .row(null, null, 9L)
                .row(null, null, 10L)
                .build();

        assertWindowQuery("row_number() OVER ()", expected);
        assertWindowQuery("row_number() OVER (ORDER BY orderkey)", expected);

        assertWindowQueryWithNulls("row_number() OVER ()", expectedWithNulls);
        assertWindowQueryWithNulls("row_number() OVER (ORDER BY orderkey)", expectedWithNulls);
    }
}
