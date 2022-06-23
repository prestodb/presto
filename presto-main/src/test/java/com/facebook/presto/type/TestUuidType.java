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
package com.facebook.presto.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.type.UuidOperators.castFromVarcharToUuid;
import static com.facebook.presto.type.UuidType.UUID;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestUuidType
        extends AbstractTestType
{
    public TestUuidType()
    {
        super(UUID, String.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = UUID.createBlockBuilder(null, 1);
        for (int i = 0; i < 10; i++) {
            String uuid = "6b5f5b65-67e4-43b0-8ee3-586cd49f58a" + i;
            UUID.writeSlice(blockBuilder, castFromVarcharToUuid(utf8Slice(uuid)));
        }
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        Slice slice = (Slice) value;
        return Slices.wrappedLongArray(slice.getLong(0), slice.getLong(1) + 1);
    }

    @Override
    protected Object getNonNullValue()
    {
        return Slices.wrappedLongArray(0, 0);
    }

    @Test
    public void testDisplayName()
    {
        assertEquals(UUID.getDisplayName(), "uuid");
    }
}
