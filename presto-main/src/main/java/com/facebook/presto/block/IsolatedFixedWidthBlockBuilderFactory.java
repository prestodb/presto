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

import com.facebook.presto.block.BlockEncoding.BlockEncodingFactory;
import com.facebook.presto.block.FixedWidthBlockUtil.FixedWidthBlockBuilderFactory;
import com.facebook.presto.block.uncompressed.FixedWidthBlockEncoding.FixedWidthBlockEncodingFactory;
import com.facebook.presto.type.FixedWidthType;

import static com.google.common.base.Preconditions.checkNotNull;

public class IsolatedFixedWidthBlockBuilderFactory
        implements FixedWidthBlockBuilderFactory
{
    private final FixedWidthType type;
    private final BlockEncodingFactory<?> blockEncodingFactory;

    public IsolatedFixedWidthBlockBuilderFactory(FixedWidthType type)
    {
        this.type = checkNotNull(type, "type is null");
        blockEncodingFactory = new FixedWidthBlockEncodingFactory(type);
    }

    @Override
    public BlockBuilder createFixedWidthBlockBuilder(BlockBuilderStatus blockBuilderStatus)
    {
        return new FixedWidthBlockBuilder(type, blockBuilderStatus);
    }

    @Override
    public BlockBuilder createFixedWidthBlockBuilder(int positionCount)
    {
        return new FixedWidthBlockBuilder(type, positionCount);
    }

    @Override
    public BlockEncodingFactory<?> getBlockEncodingFactory()
    {
        return blockEncodingFactory;
    }
}
