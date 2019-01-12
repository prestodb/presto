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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.type.ArrayType;
import org.testng.annotations.Test;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;

public class TestApproxPercentileWindow
        extends AbstractTestWindowFunction
{
    @Test
    public void testDoubleApproxPercentile()
    {
        assertWindowQuery("approx_percentile(DOUBLE '42', 0.5) over ()",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, DOUBLE)
                        .row(1, "O", 42.0d)
                        .row(2, "O", 42.0d)
                        .row(3, "F", 42.0d)
                        .row(4, "O", 42.0d)
                        .row(5, "F", 42.0d)
                        .row(6, "F", 42.0d)
                        .row(7, "O", 42.0d)
                        .row(32, "O", 42.0d)
                        .row(33, "F", 42.0d)
                        .row(34, "O", 42.0d)
                        .build());
    }

    @Test
    public void testLongArrayApproxPercentile()
    {
        assertWindowQuery("approx_percentile(12, array[0.3, 0.7]) over ()",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, new ArrayType(BIGINT))
                        .row(1, "O", ImmutableList.of(12L, 12L))
                        .row(2, "O", ImmutableList.of(12L, 12L))
                        .row(3, "F", ImmutableList.of(12L, 12L))
                        .row(4, "O", ImmutableList.of(12L, 12L))
                        .row(5, "F", ImmutableList.of(12L, 12L))
                        .row(6, "F", ImmutableList.of(12L, 12L))
                        .row(7, "O", ImmutableList.of(12L, 12L))
                        .row(32, "O", ImmutableList.of(12L, 12L))
                        .row(33, "F", ImmutableList.of(12L, 12L))
                        .row(34, "O", ImmutableList.of(12L, 12L))
                        .build());
    }

    @Test
    public void testDoubleArrayApproxPercentile()
    {
        assertWindowQuery("approx_percentile(DOUBLE '42.3', array[0.5]) over ()",
                resultBuilder(TEST_SESSION, INTEGER, VARCHAR, new ArrayType(DOUBLE))
                        .row(1, "O", ImmutableList.of(42.3d))
                        .row(2, "O", ImmutableList.of(42.3d))
                        .row(3, "F", ImmutableList.of(42.3d))
                        .row(4, "O", ImmutableList.of(42.3d))
                        .row(5, "F", ImmutableList.of(42.3d))
                        .row(6, "F", ImmutableList.of(42.3d))
                        .row(7, "O", ImmutableList.of(42.3d))
                        .row(32, "O", ImmutableList.of(42.3d))
                        .row(33, "F", ImmutableList.of(42.3d))
                        .row(34, "O", ImmutableList.of(42.3d))
                        .build());
    }
}
