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
package com.facebook.presto.operator.aggregation.histogram;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.TypeUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestValueStore
{
    private ValueStore valueStore;
    private Block block1;
    private Block block2;
    private VarcharType type;
    private BlockBuilder blockBuilder;
    private ValueStore valueStoreSmall;

    @BeforeMethod(alwaysRun = true)
    public void setUp()
            throws Exception
    {
        type = VarcharType.createVarcharType(100);
        blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 100, 10);
        valueStore = new ValueStore(100, blockBuilder);
        valueStoreSmall = new ValueStore(1, blockBuilder);
        block1 = BlockAssertions.createStringsBlock("a", "b", "c", "d");
    }

    @Test
    public void testUniqueness()
            throws Exception
    {
        assertEquals(valueStore.addAndGetPosition(type, block1, 0, TypeUtils.hashPosition(type, block1, 0)), 0);
        assertEquals(valueStore.addAndGetPosition(type, block1, 1, TypeUtils.hashPosition(type, block1, 1)), 1);
        assertEquals(valueStore.addAndGetPosition(type, block1, 2, TypeUtils.hashPosition(type, block1, 2)), 2);
        assertEquals(valueStore.addAndGetPosition(type, block1, 1, TypeUtils.hashPosition(type, block1, 1)), 1);
        assertEquals(valueStore.addAndGetPosition(type, block1, 3, TypeUtils.hashPosition(type, block1, 1)), 3);
    }

    @Test
    public void testTriggerRehash()
            throws Exception
    {
        long hash0 = TypeUtils.hashPosition(type, block1, 0);
        long hash1 = TypeUtils.hashPosition(type, block1, 1);
        long hash2 = TypeUtils.hashPosition(type, block1, 2);
        assertEquals(valueStoreSmall.addAndGetPosition(type, block1, 0, hash0), 0);
        assertEquals(valueStoreSmall.addAndGetPosition(type, block1, 1, hash1), 1);
        // triggers rehash and hahs1 will end up in position 3
        assertEquals(valueStoreSmall.addAndGetPosition(type, block1, 2, hash2), 2);
        // this is just to make sure we trigger rehash code positions should be the same
        assertTrue(valueStoreSmall.getRehashCount() > 0);
        assertEquals(valueStoreSmall.addAndGetPosition(type, block1, 0, hash0), 0);
        assertEquals(valueStoreSmall.addAndGetPosition(type, block1, 1, hash1), 1);
        assertEquals(valueStoreSmall.addAndGetPosition(type, block1, 2, hash2), 2);
    }
}
