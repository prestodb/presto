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
package com.facebook.presto.block;

import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockBuilder.DEFAULT_MAX_BLOCK_SIZE;
import static com.facebook.presto.type.Types.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestBlockBuilder
{
    @Test
    public void testMultipleValuesWithNull()
    {
        BlockCursor cursor = BIGINT.createBlockBuilder(DEFAULT_MAX_BLOCK_SIZE)
                .appendNull()
                .append(42)
                .appendNull()
                .append(42)
                .build()
                .cursor();

        assertTrue(cursor.advanceNextPosition());
        assertTrue(cursor.isNull());

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(), 42L);

        assertTrue(cursor.advanceNextPosition());
        assertTrue(cursor.isNull());

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(), 42L);
    }
}
