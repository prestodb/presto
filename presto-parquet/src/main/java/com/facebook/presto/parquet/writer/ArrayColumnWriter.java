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
package com.facebook.presto.parquet.writer;

import com.facebook.presto.common.block.ColumnarArray;
import com.facebook.presto.parquet.writer.levels.DefinitionLevelIterable;
import com.facebook.presto.parquet.writer.levels.DefinitionLevelIterables;
import com.facebook.presto.parquet.writer.levels.RepetitionLevelIterable;
import com.facebook.presto.parquet.writer.levels.RepetitionLevelIterables;
import com.google.common.collect.ImmutableList;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ArrayColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayColumnWriter.class).instanceSize();

    private final ColumnWriter elementWriter;
    private final int maxDefinitionLevel;
    private final int maxRepetitionLevel;

    public ArrayColumnWriter(ColumnWriter elementWriter, int maxDefinitionLevel, int maxRepetitionLevel)
    {
        this.elementWriter = requireNonNull(elementWriter, "elementWriter is null");
        this.maxDefinitionLevel = maxDefinitionLevel;
        this.maxRepetitionLevel = maxRepetitionLevel;
    }

    @Override
    public void writeBlock(ColumnChunk columnChunk)
            throws IOException
    {
        ColumnarArray columnarArray = ColumnarArray.toColumnarArray(columnChunk.getBlock());
        elementWriter.writeBlock(
                new ColumnChunk(columnarArray.getElementsBlock(),
                        ImmutableList.<DefinitionLevelIterable>builder()
                                .addAll(columnChunk.getDefinitionLevelIterables())
                                .add(DefinitionLevelIterables.of(columnarArray, maxDefinitionLevel))
                                .build(),
                        ImmutableList.<RepetitionLevelIterable>builder()
                                .addAll(columnChunk.getRepetitionLevelIterables())
                                .add(RepetitionLevelIterables.of(columnarArray, maxRepetitionLevel))
                                .build()));
    }

    @Override
    public void close()
    {
        elementWriter.close();
    }

    @Override
    public List<BufferData> getBuffer()
            throws IOException
    {
        return ImmutableList.copyOf(elementWriter.getBuffer());
    }

    @Override
    public long getBufferedBytes()
    {
        return elementWriter.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + elementWriter.getRetainedBytes();
    }

    @Override
    public void reset()
    {
        elementWriter.reset();
    }
}
