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

package com.facebook.presto.util;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestLongValueCompressor
{
    @Test
    public void happyPath()
    {
        assertEquals(new LongValueCompressor().build(0), 0L);
        assertEquals(new LongValueCompressor().build((long) Integer.MAX_VALUE + 1), (long) Integer.MAX_VALUE + 1L);
        assertEquals(new LongValueCompressor().store(0, 4).build(0), 0L);
        assertEquals(new LongValueCompressor().store(0, 4).build(1), 1L);
        assertEquals(new LongValueCompressor().store(3, 4).build(1), 0x3000000000000001L);
        assertEquals(new LongValueCompressor().store(1, 4).store(1, 4).build(1), 0x1100000000000001L);
        assertEquals(new LongValueCompressor().store(1, 4).store(11, 4).store(15, 4).build(1), 0x1BF0000000000001L);
        assertEquals(new LongValueCompressor().store(Integer.MAX_VALUE, 32).build(Integer.MAX_VALUE), 0x7fffffff7fffffffL);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void unableToStoreHigherValueThanCanFitIntoBits()
    {
        new LongValueCompressor().store(2, 1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void unableToStoreMoreBitsThanLongHas()
    {
        new LongValueCompressor().store(2, 32).store(2, 32).store(1, 1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void unableToBuildWhenAllBitsAreUsed()
    {
        new LongValueCompressor().store(2, 32).store(2, 32).build(1);
    }
}
