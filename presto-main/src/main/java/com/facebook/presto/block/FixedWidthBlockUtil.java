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

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding.BlockEncodingFactory;
import com.facebook.presto.block.uncompressed.AbstractFixedWidthBlock;
import com.facebook.presto.block.uncompressed.FixedWidthBlock;
import com.facebook.presto.block.uncompressed.FixedWidthBlockCursor;
import com.facebook.presto.block.uncompressed.FixedWidthBlockEncoding;
import com.facebook.presto.operator.aggregation.IsolatedClass;
import com.facebook.presto.type.FixedWidthType;
import com.google.common.base.Throwables;

public class FixedWidthBlockUtil
{
    private FixedWidthBlockUtil()
    {
    }

    public interface FixedWidthBlockBuilderFactory
    {
        BlockBuilder createFixedWidthBlockBuilder(BlockBuilderStatus blockBuilderStatus);

        BlockBuilder createFixedWidthBlockBuilder(int positionCount);

        BlockEncodingFactory<?> getBlockEncodingFactory();
    }

    public static FixedWidthBlockBuilderFactory createIsolatedFixedWidthBlockBuilderFactory(FixedWidthType type)
    {
        Class<? extends FixedWidthBlockBuilderFactory> functionClass = IsolatedClass.isolateClass(
                FixedWidthBlockBuilderFactory.class,
                IsolatedFixedWidthBlockBuilderFactory.class,

                AbstractFixedWidthBlock.class,
                FixedWidthBlock.class,
                FixedWidthBlockBuilder.class,

                FixedWidthBlockCursor.class,

                FixedWidthBlockEncoding.class);

        try {
            return functionClass
                    .getConstructor(FixedWidthType.class)
                    .newInstance(type);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
