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
package com.facebook.presto.common.block;

import io.airlift.slice.SliceTooLargeException;
import org.testng.annotations.Test;

import static com.facebook.presto.common.block.BlockUtil.MAX_ARRAY_SIZE;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestBlockUtil
{
    @Test
    public void testCalculateNewArraySize()
    {
        assertEquals(BlockUtil.calculateNewArraySize(200), 300);
        assertEquals(BlockUtil.calculateNewArraySize(Integer.MAX_VALUE), MAX_ARRAY_SIZE);
        try {
            BlockUtil.calculateNewArraySize(MAX_ARRAY_SIZE);
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), format("Can not grow array beyond '%s'", MAX_ARRAY_SIZE));
        }
    }

    @Test(expectedExceptions = SliceTooLargeException.class, expectedExceptionsMessageRegExp = "Cannot allocate slice larger than 2147483639 bytes")
    public void testCheckInValidSliceRange()
    {
        BlockUtil.checkValidSliceRange(MAX_ARRAY_SIZE - 10, 11);
    }

    @Test
    public void testCheckValidSliceRange()
    {
        BlockUtil.checkValidSliceRange(MAX_ARRAY_SIZE - 10, 9);
    }
}
