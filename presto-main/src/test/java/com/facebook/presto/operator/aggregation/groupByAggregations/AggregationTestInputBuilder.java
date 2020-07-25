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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;

public class AggregationTestInputBuilder
{
    private final InternalAggregationFunction function;

    private int offset = -1;
    private boolean isRerversed;
    private Page[] pages;

    public AggregationTestInputBuilder(Block[] blocks, InternalAggregationFunction function)
    {
        this.pages = GroupByAggregationTestUtils.createPages(blocks);
        this.function = function;
    }

    public AggregationTestInputBuilder setOffset(int offset)
    {
        this.offset = offset;

        return this;
    }

    public AggregationTestInputBuilder setPages(Page[] pages)
    {
        this.pages = pages;

        return this;
    }

    public AggregationTestInputBuilder setRerversed(boolean rerversed)
    {
        isRerversed = rerversed;

        return this;
    }

    public AggregationTestInput build()
    {
        return new AggregationTestInput(function, pages, offset, isRerversed);
    }
}
