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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractColumnBuilder
        implements ColumnBuilder
{
    private final int channel;
    private final Type type;
    private final boolean disableEncoding;
    private final List<Segment> segmentBuilders;
    private SegmentBuilder currentBuilder;
    private long rowCount;
    private long sizeBytes;

    public AbstractColumnBuilder(int channel, Type type, boolean disableEncoding)
    {
        this.channel = channel;
        this.type = type;
        this.disableEncoding = disableEncoding;
        segmentBuilders = new ArrayList<>();
        currentBuilder = createSegmentBuilder();
    }

    protected abstract SegmentBuilder createSegmentBuilder();

    public int getChannel()
    {
        return channel;
    }

    public Type getType()
    {
        return type;
    }

    protected boolean getDisableEncoding()
    {
        return disableEncoding;
    }

    @Override
    public void appendPage(Page page)
    {
        Block block = page.getBlock(channel);
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (currentBuilder.isFull()) {
                Segment segment = currentBuilder.build();
                segmentBuilders.add(segment);
                rowCount += segment.size();
                sizeBytes += segment.getSizeBytes();
                currentBuilder = createSegmentBuilder();
            }
            currentBuilder.append(block, position);
        }
    }

    public Column build()
    {
        if (!currentBuilder.isEmpty()) {
            Segment segment = currentBuilder.build();
            segmentBuilders.add(segment);
            rowCount += segment.size();
            sizeBytes += segment.getSizeBytes();
        }
        return new Column(segmentBuilders.toArray(new Segment[0]), rowCount, sizeBytes);
    }
}
