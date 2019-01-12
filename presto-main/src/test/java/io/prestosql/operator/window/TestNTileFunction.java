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
package io.prestosql.operator.window;

import org.testng.annotations.Test;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;

public class TestNTileFunction
        extends AbstractTestWindowFunction
{
    @Test
    public void testNTile()
    {
        assertWindowQuery("ntile(4) OVER (ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(1, "O", 1L)
                        .row(2, "O", 1L)
                        .row(3, "F", 1L)
                        .row(4, "O", 2L)
                        .row(5, "F", 2L)
                        .row(6, "F", 2L)
                        .row(7, "O", 3L)
                        .row(32, "O", 3L)
                        .row(33, "F", 4L)
                        .row(34, "O", 4L)
                        .build());

        assertWindowQuery("ntile(6) OVER (ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(1, "O", 1L)
                        .row(2, "O", 1L)
                        .row(3, "F", 2L)
                        .row(4, "O", 2L)
                        .row(5, "F", 3L)
                        .row(6, "F", 3L)
                        .row(7, "O", 4L)
                        .row(32, "O", 4L)
                        .row(33, "F", 5L)
                        .row(34, "O", 6L)
                        .build());

        assertWindowQuery("ntile(20) OVER (ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
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
                        .build());

        assertWindowQuery("ntile(orderkey) OVER (ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(1, "O", 1L)
                        .row(2, "O", 1L)
                        .row(3, "F", 1L)
                        .row(4, "O", 2L)
                        .row(5, "F", 3L)
                        .row(6, "F", 3L)
                        .row(7, "O", 4L)
                        .row(32, "O", 8L)
                        .row(33, "F", 9L)
                        .row(34, "O", 10L)
                        .build());
        assertWindowQueryWithNulls("ntile(orderkey) OVER (ORDER BY orderkey)",
                resultBuilder(TEST_SESSION, BIGINT, VARCHAR, BIGINT)
                        .row(1L, null, 1L)
                        .row(3L, "F", 1L)
                        .row(5L, "F", 2L)
                        .row(6L, "F", 2L)
                        .row(7L, null, 3L)
                        .row(34L, "O", 6L)
                        .row(null, "F", null)
                        .row(null, "O", null)
                        .row(null, null, null)
                        .row(null, null, null)
                        .build());
    }
}
