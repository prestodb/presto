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

import static org.testng.Assert.assertEquals;

public class GroupByAggregationTestUtils
{
    private GroupByAggregationTestUtils()
    {
        throw new UnsupportedOperationException("util class only");
    }

    public static Page[] createPages(Block[] blocks)
    {
        int positions = blocks[0].getPositionCount();
        for (int i = 1; i < blocks.length; i++) {
            assertEquals(positions, blocks[i].getPositionCount(), "input blocks provided are not equal in position count");
        }
        if (positions == 0) {
            return new Page[] {};
        }
        else if (positions == 1) {
            return new Page[] {new Page(positions, blocks)};
        }
        else {
            int split = positions / 2; // [0, split - 1] goes to first list of blocks; [split, positions - 1] goes to second list of blocks.
            Block[] blockArray1 = new Block[blocks.length];
            Block[] blockArray2 = new Block[blocks.length];
            for (int i = 0; i < blocks.length; i++) {
                blockArray1[i] = blocks[i].getRegion(0, split);
                blockArray2[i] = blocks[i].getRegion(split, positions - split);
            }
            return new Page[] {new Page(blockArray1), new Page(blockArray2)};
        }
    }

    public static int[] createArgs(InternalAggregationFunction function)
    {
        int[] args = new int[function.getParameterTypes().size()];
        for (int i = 0; i < args.length; i++) {
            args[i] = i;
        }
        return args;
    }
}
