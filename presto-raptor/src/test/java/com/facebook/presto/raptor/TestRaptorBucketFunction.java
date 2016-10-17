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

import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createIntsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.raptor.RaptorBucketFunction.bigintHashFunction;
import static com.facebook.presto.raptor.RaptorBucketFunction.intHashFunction;
import static com.facebook.presto.raptor.RaptorBucketFunction.varcharHashFunction;
import static org.testng.Assert.assertEquals;

public class TestRaptorBucketFunction
{
    @Test
    public void testBigintHash()
    {
        assertEquals(bigintHashFunction().hash(createLongsBlock(454345325), 0), -1124888032984160314L);
        assertEquals(bigintHashFunction().hash(createLongsBlock(365363), 0), -8712652669558601392L);
        assertEquals(bigintHashFunction().hash(createLongsBlock(45645747), 0), -566337585468696979L);
        assertEquals(bigintHashFunction().hash(createLongsBlock(3244), 0), 7166483564661250355L);
    }

    @Test
    public void testIntegerHash()
    {
        assertEquals(intHashFunction().hash(createIntsBlock(1435543542), 0), -8438568885860948000L);
        assertEquals(intHashFunction().hash(createIntsBlock(4324), 0), 7993471868943095241L);
        assertEquals(intHashFunction().hash(createIntsBlock(64563), 0), -1561939044676042257L);
        assertEquals(intHashFunction().hash(createIntsBlock(45736575), 0), 8777360711768080356L);
    }

    @Test
    public void testVarcharHash()
    {
        assertEquals(varcharHashFunction().hash(createStringsBlock("lorem ipsum"), 0), -962685959067592082L);
        assertEquals(varcharHashFunction().hash(createStringsBlock("lorem"), 0), 5511943272777369178L);
        assertEquals(varcharHashFunction().hash(createStringsBlock("ipsum"), 0), 8014067751226690893L);
        assertEquals(varcharHashFunction().hash(createStringsBlock("hello"), 0), 2794345569481354659L);
    }
}
