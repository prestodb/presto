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

package com.facebook.presto.operator.aggregation.groupByAggregations;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;

import java.util.function.BiConsumer;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class AggregationTestOutput
{
    private final Object expectedValue;

    public AggregationTestOutput(Object expectedValue)
    {
        this.expectedValue = expectedValue;
    }

    public void validateAccumulator(GroupedAccumulator groupedAccumulator, long groupId)
    {
        createEqualAssertion(expectedValue, groupId).accept(getGroupValue(groupedAccumulator, (int) groupId), expectedValue);
    }

    private static BiConsumer<Object, Object> createEqualAssertion(Object expectedValue, long groupId)

    {
        BiConsumer<Object, Object> equalAssertion = (actual, expected) -> assertEquals(actual, expected, format("failure on group %s", groupId));

        if (expectedValue instanceof Double && !expectedValue.equals(Double.NaN)) {
            equalAssertion = (actual, expected) -> assertEquals((double) actual, (double) expected, 1e-10);
        }
        if (expectedValue instanceof Float && !expectedValue.equals(Float.NaN)) {
            equalAssertion = (actual, expected) -> assertEquals((float) actual, (float) expected, 1e-10f);
        }
        return equalAssertion;
    }

    private static Object getGroupValue(GroupedAccumulator groupedAggregation, int groupId)
    {
        BlockBuilder out = groupedAggregation.getFinalType().createBlockBuilder(null, 1);
        groupedAggregation.evaluateFinal(groupId, out);
        return BlockAssertions.getOnlyValue(groupedAggregation.getFinalType(), out.build());
    }
}
