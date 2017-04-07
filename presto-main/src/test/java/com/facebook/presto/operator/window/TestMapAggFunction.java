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

import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;

public class TestMapAggFunction
        extends AbstractTestWindowFunction
{
    @Test
    public void testMapAgg()
    {
        assertWindowQuery("map_agg(orderkey, orderstatus) OVER(PARTITION BY orderdate)",
                resultBuilder(TEST_SESSION, BIGINT, VarcharType.createVarcharType(1), new MapType(BIGINT, VarcharType.createVarcharType(1)))
                        .row(1, "O", ImmutableMap.of(1, "O"))
                        .row(2, "O", ImmutableMap.of(2, "O"))
                        .row(3, "F", ImmutableMap.of(3, "F"))
                        .row(4, "O", ImmutableMap.of(4, "O"))
                        .row(5, "F", ImmutableMap.of(5, "F"))
                        .row(6, "F", ImmutableMap.of(6, "F"))
                        .row(7, "O", ImmutableMap.of(7, "O"))
                        .row(32, "O", ImmutableMap.of(32, "O"))
                        .row(33, "F", ImmutableMap.of(33, "F"))
                        .row(34, "O", ImmutableMap.of(34, "O"))
                        .build());
    }
}
