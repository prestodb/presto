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

import com.facebook.presto.common.type.VarcharType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.util.StructuralTestUtil.mapType;

public class TestMinMaxNFunction
        extends AbstractTestWindowFunction
{
    @Test
    public void testMaxByGlobalWindow()
    {
        assertWindowQuery(
                "max(orderkey, 3) OVER()",
                resultBuilder(TEST_SESSION, BIGINT, VarcharType.createVarcharType(1), mapType(BIGINT, VarcharType.createVarcharType(1)))
                        .row(1, "O", ImmutableList.of(34, 33, 32))
                        .row(2, "O", ImmutableList.of(34, 33, 32))
                        .row(3, "F", ImmutableList.of(34, 33, 32))
                        .row(4, "O", ImmutableList.of(34, 33, 32))
                        .row(5, "F", ImmutableList.of(34, 33, 32))
                        .row(6, "F", ImmutableList.of(34, 33, 32))
                        .row(7, "O", ImmutableList.of(34, 33, 32))
                        .row(32, "O", ImmutableList.of(34, 33, 32))
                        .row(33, "F", ImmutableList.of(34, 33, 32))
                        .row(34, "O", ImmutableList.of(34, 33, 32))
                        .build());
    }

    @Test
    public void testMaxByWithPartition()
    {
        assertWindowQuery(
                "max(orderkey, 3) OVER(PARTITION BY orderstatus)",
                resultBuilder(TEST_SESSION, BIGINT, VarcharType.createVarcharType(1), mapType(BIGINT, VarcharType.createVarcharType(1)))
                        .row(1, "O", ImmutableList.of(34, 32, 7))
                        .row(2, "O", ImmutableList.of(34, 32, 7))
                        .row(3, "F", ImmutableList.of(33, 6, 5))
                        .row(4, "O", ImmutableList.of(34, 32, 7))
                        .row(5, "F", ImmutableList.of(33, 6, 5))
                        .row(6, "F", ImmutableList.of(33, 6, 5))
                        .row(7, "O", ImmutableList.of(34, 32, 7))
                        .row(32, "O", ImmutableList.of(34, 32, 7))
                        .row(33, "F", ImmutableList.of(33, 6, 5))
                        .row(34, "O", ImmutableList.of(34, 32, 7))
                        .build());
    }
}
