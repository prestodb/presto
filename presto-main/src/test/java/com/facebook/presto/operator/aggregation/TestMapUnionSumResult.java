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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.MapBlock;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.operator.aggregation.state.MapUnionSumState;
import com.facebook.presto.operator.aggregation.state.MapUnionSumStateFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createMapBlock;
import static com.facebook.presto.block.BlockAssertions.createMapType;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestMapUnionSumResult
{
    @Test
    public void testAddToKeySet()
    {
        MapType mapType = createMapType(BIGINT, BIGINT);
        Map<Long, Long> map = new HashMap<>();
        map.put(1L, 1L);
        MapBlock mapBlock = (MapBlock) createMapBlock(mapType, map);
        Block singleMapBlock = mapBlock.getBlock(0);
        MapUnionSumState mapUnionSumState = new MapUnionSumStateFactory(BIGINT, BIGINT).createSingleState();
        MapUnionSumResult mapUnionSumResult = MapUnionSumResult.create(BIGINT, BIGINT, mapUnionSumState.getAdder(), singleMapBlock);
        TypedSet typedSet = new TypedSet(BIGINT, 1, "TEST");
        Block block = createLongsBlock(-1);
        typedSet.add(block, 0);
        assertEquals(typedSet.size(), 1);
        assertEquals(mapUnionSumResult.size(), 1);
        mapUnionSumResult.unionSum(mapUnionSumResult).addKeyToSet(typedSet, 0);
        assertEquals(typedSet.size(), 2);
        assertEquals(mapUnionSumResult.size(), 1);
    }
}
