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
package com.facebook.presto.hive.pagefile;

import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.spi.page.PageDataOutput;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.openjdk.jol.info.ClassLayout;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class PageWriter
        implements Closeable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PageWriter.class).instanceSize();

    private final DataSink dataSink;
    private final HiveCompressionCodec compressionCodec;
    private final AtomicBoolean closed = new AtomicBoolean();
    private long bufferedBytes;
    private long retainedBytes;
    private long maxBufferedBytes;
    private List<DataOutput> bufferedPages = new ArrayList<>();
    private List<Long> stripeOffsets = new ArrayList<>();
    private long stripeOffset;

    public PageWriter(
            DataSink dataSink,
            HiveCompressionCodec compressionCodec,
            DataSize pageFileStripeMaxSize)
    {
        this.dataSink = requireNonNull(dataSink, "pageDataSink is null");
        this.compressionCodec = requireNonNull(compressionCodec, "compressionCodec is null");
        this.maxBufferedBytes = requireNonNull(pageFileStripeMaxSize, "pageFileStripeMaxSize is null").toBytes();
    }

    /**
     * Number of bytes already flushed to the data sink.
     */
    public long getWrittenBytes()
    {
        return dataSink.size();
    }

    public void write(SerializedPage page)
            throws IOException
    {
        PageDataOutput pageDataOutput = new PageDataOutput(page);
        long writtenSize = pageDataOutput.size();
        if (maxBufferedBytes - bufferedBytes < writtenSize) {
            flushStripe();
        }
        bufferedPages.add(pageDataOutput);
        bufferedBytes += writtenSize;
        retainedBytes += page.getRetainedSizeInBytes();
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (!bufferedPages.isEmpty()) {
            flushStripe();
        }
        dataSink.write(ImmutableList.of(new PageFileFooterOutput(stripeOffsets, compressionCodec)));
        dataSink.close();
    }

    public void closeWithoutWrite()
            throws IOException
    {
        if (closed.compareAndSet(false, true)) {
            dataSink.close();
        }
    }

    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + retainedBytes + dataSink.getRetainedSizeInBytes();
    }

    private void flushStripe()
            throws IOException
    {
        dataSink.write(bufferedPages);
        stripeOffsets.add(stripeOffset);
        stripeOffset += bufferedBytes;
        bufferedPages.clear();
        bufferedBytes = 0;
        retainedBytes = 0;
    }
}
