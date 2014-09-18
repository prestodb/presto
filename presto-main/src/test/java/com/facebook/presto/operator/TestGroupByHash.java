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
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestGroupByHash
{
    private static final int MAX_GROUP_ID = 500;

    @Test
    public void test()
            throws Exception
    {
        GroupByHash groupByHash = new GroupByHash(ImmutableList.of(BIGINT), new int[] {0}, 100);
        for (int tries = 0; tries < 2; tries++) {
            for (int value = 0; value < MAX_GROUP_ID; value++) {
                Page page = new Page(BlockAssertions.createLongsBlock(value));
                for (int addValuesTries = 0; addValuesTries < 10; addValuesTries++) {
                    GroupByIdBlock groupIds = groupByHash.getGroupIds(page);
                    assertEquals(groupIds.getGroupCount(), tries == 0 ? value + 1 : MAX_GROUP_ID);
                    assertEquals(groupIds.getPositionCount(), 1);
                    long groupId = groupIds.getGroupId(0);
                    assertEquals(groupId, value);
                }
            }
        }
    }
}
