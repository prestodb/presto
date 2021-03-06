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

import com.facebook.presto.common.Page;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.UpdateMemory;
import com.facebook.presto.operator.aggregation.AggregationTestUtils;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.google.common.base.Suppliers;
import org.testng.internal.collections.Ints;

import java.util.Optional;
import java.util.function.Supplier;

public class AggregationTestInput
{
    private final Page[] pages;
    private final InternalAggregationFunction function;
    private int[] args;

    private final int offset;
    private final boolean isReversed;

    @SuppressWarnings("NumericCastThatLosesPrecision")
    public AggregationTestInput(InternalAggregationFunction function, Page[] pages, int offset, boolean isReversed)
    {
        this.pages = pages;
        this.function = function;
        args = GroupByAggregationTestUtils.createArgs(function);
        this.offset = offset;
        this.isReversed = isReversed;
    }

    public void runPagesOnAccumulatorWithAssertion(long groupId, GroupedAccumulator groupedAccumulator, AggregationTestOutput expectedValue)
    {
        GroupedAccumulator accumulator = Suppliers.ofInstance(groupedAccumulator).get();

        for (Page page : getPages()) {
            accumulator.addInput(getGroupIdBlock(groupId, page), page);
        }

        expectedValue.validateAccumulator(accumulator, groupId);
    }

    public GroupedAccumulator runPagesOnAccumulator(long groupId, GroupedAccumulator groupedAccumulator)
    {
        return runPagesOnAccumulator(groupId, Suppliers.ofInstance(groupedAccumulator));
    }

    public GroupedAccumulator runPagesOnAccumulator(long groupId, Supplier<GroupedAccumulator> accumulatorSupplier)
    {
        GroupedAccumulator accumulator = accumulatorSupplier.get();

        for (Page page : getPages()) {
            accumulator.addInput(getGroupIdBlock(groupId, page), page);
        }

        return accumulator;
    }

    private GroupByIdBlock getGroupIdBlock(long groupId, Page page)
    {
        return AggregationTestUtils.createGroupByIdBlock((int) groupId, page.getPositionCount());
    }

    private Page[] getPages()
    {
        Page[] pages = this.pages;

        if (isReversed) {
            pages = AggregationTestUtils.reverseColumns(pages);
        }

        if (offset > 0) {
            pages = AggregationTestUtils.offsetColumns(pages, offset);
        }

        return pages;
    }

    public GroupedAccumulator createGroupedAccumulator()
    {
        return function.bind(Ints.asList(args), Optional.empty())
                .createGroupedAccumulator(UpdateMemory.NOOP);
    }
}
