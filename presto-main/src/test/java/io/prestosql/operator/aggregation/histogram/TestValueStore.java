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
package io.prestosql.operator.aggregation.histogram;

import io.prestosql.block.BlockAssertions;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.type.TypeUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestValueStore
{
    private ValueStore valueStore;
    private Block block;
    private VarcharType type;
    private ValueStore valueStoreSmall;

    @BeforeMethod(alwaysRun = true)
    public void setUp()
            throws Exception
    {
        type = VarcharType.createVarcharType(100);
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 100, 10);
        valueStore = new ValueStore(100, blockBuilder);
        valueStoreSmall = new ValueStore(1, blockBuilder);
        block = BlockAssertions.createStringsBlock("a", "b", "c", "d");
    }

    @Test
    public void testUniqueness()
    {
        assertEquals(valueStore.addAndGetPosition(type, block, 0, TypeUtils.hashPosition(type, block, 0)), 0);
        assertEquals(valueStore.addAndGetPosition(type, block, 1, TypeUtils.hashPosition(type, block, 1)), 1);
        assertEquals(valueStore.addAndGetPosition(type, block, 2, TypeUtils.hashPosition(type, block, 2)), 2);
        assertEquals(valueStore.addAndGetPosition(type, block, 1, TypeUtils.hashPosition(type, block, 1)), 1);
        assertEquals(valueStore.addAndGetPosition(type, block, 3, TypeUtils.hashPosition(type, block, 1)), 3);
    }

    @Test
    public void testTriggerRehash()
    {
        long hash0 = TypeUtils.hashPosition(type, block, 0);
        long hash1 = TypeUtils.hashPosition(type, block, 1);
        long hash2 = TypeUtils.hashPosition(type, block, 2);

        assertEquals(valueStoreSmall.addAndGetPosition(type, block, 0, hash0), 0);
        assertEquals(valueStoreSmall.addAndGetPosition(type, block, 1, hash1), 1);

        // triggers rehash and hash1 will end up in position 3
        assertEquals(valueStoreSmall.addAndGetPosition(type, block, 2, hash2), 2);

        // this is just to make sure we trigger rehash code positions should be the same
        assertTrue(valueStoreSmall.getRehashCount() > 0);
        assertEquals(valueStoreSmall.addAndGetPosition(type, block, 0, hash0), 0);
        assertEquals(valueStoreSmall.addAndGetPosition(type, block, 1, hash1), 1);
        assertEquals(valueStoreSmall.addAndGetPosition(type, block, 2, hash2), 2);
    }
}
