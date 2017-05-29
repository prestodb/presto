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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import io.airlift.slice.Slice;

import java.util.UUID;

import static com.facebook.presto.type.UuidType.UUID_TYPE;

public class TestUuidType
        extends AbstractTestType
{
    public TestUuidType()
    {
        super(UUID_TYPE, UUID.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = UUID_TYPE.createBlockBuilder(new BlockBuilderStatus(), 10);
        UUID_TYPE.writeObject(blockBuilder, UUID.randomUUID());
        UUID_TYPE.writeObject(blockBuilder, UUID.randomUUID());
        UUID_TYPE.writeObject(blockBuilder, UUID.randomUUID());

        UUID_TYPE.writeObject(blockBuilder, UUID.randomUUID());
        UUID_TYPE.writeObject(blockBuilder, UUID.randomUUID());
        UUID_TYPE.writeObject(blockBuilder, UUID.randomUUID());

        UUID_TYPE.writeObject(blockBuilder, UUID.randomUUID());
        UUID_TYPE.writeObject(blockBuilder, UUID.randomUUID());
        UUID_TYPE.writeObject(blockBuilder, UUID.randomUUID());
        UUID_TYPE.writeObject(blockBuilder, UUID.randomUUID());

        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        UUID uuid = UuidType.uuidFromSlice((Slice) value);
        UUID greaterUuid = new UUID(uuid.getMostSignificantBits() + 1, uuid.getLeastSignificantBits());
        return UuidType.uuidToSlice(greaterUuid);
    }

    @Override
    protected Object getNonNullValue()
    {
        return UuidType.uuidToSlice(UUID.randomUUID());
    }
}
