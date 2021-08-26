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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ColumnWriter
{
    long NULL_SIZE = 1;

    default List<ColumnWriter> getNestedColumnWriters()
    {
        return ImmutableList.of();
    }

    Map<Integer, ColumnEncoding> getColumnEncodings();

    void beginRowGroup();

    long writeBlock(Block block);

    Map<Integer, ColumnStatistics> finishRowGroup();

    void close();

    Map<Integer, ColumnStatistics> getColumnStripeStatistics();

    /**
     * Write index streams to the output and return the streams in the
     * order in which they were written.  The ordering is critical because
     * the stream only contain a length with no offset.
     */
    List<StreamDataOutput> getIndexStreams()
            throws IOException;

    /**
     * Get the data streams to be written.
     */
    List<StreamDataOutput> getDataStreams();

    /**
     * This method returns the size of the flushed data plus any unflushed data.
     * If the output is compressed, flush data size is the size after compression.
     */
    long getBufferedBytes();

    long getRetainedBytes();

    void reset();
}
