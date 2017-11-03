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
package com.facebook.presto.plugin.turbonium.storage;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.type.Type;

public class NullSegment
        implements Segment
{
    private final int size;
    private final Domain domain;
    public NullSegment(Type type, int size)
    {
        this.size = size;
        this.domain = Domain.onlyNull(type);
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public void write(BlockBuilder blockBuilder, int position)
    {
        blockBuilder.appendNull();
    }

    @Override
    public Domain getDomain()
    {
        return domain;
    }

    @Override
    public long getSizeBytes()
    {
        return 0L;
    }
}
