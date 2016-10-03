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
package com.facebook.presto.raptor;

import com.facebook.presto.block.BlockAssertions;
import org.testng.annotations.Test;

import static com.facebook.presto.raptor.RaptorBucketFunction.bigintHashFunction;
import static com.facebook.presto.raptor.RaptorBucketFunction.intHashFunction;
import static com.facebook.presto.raptor.RaptorBucketFunction.varcharHashFunction;
import static org.testng.Assert.assertEquals;

public class TestShardHashing
{
    @Test
    public void testBigintHash()
    {
        assertEquals(bigintHashFunction().hash(BlockAssertions.createLongsBlock(454345325), 0), -1124888032984160314L);
    }

    @Test
    public void testVarcharHash()
    {
        assertEquals(varcharHashFunction().hash(BlockAssertions.createStringsBlock("lorem ipsum"), 0), -962685959067592082L);
    }

    @Test
    public void testIntegerHash()
    {
        assertEquals(intHashFunction().hash(BlockAssertions.createIntsBlock(1435543542), 0), -8438568885860948000L);
    }
}
