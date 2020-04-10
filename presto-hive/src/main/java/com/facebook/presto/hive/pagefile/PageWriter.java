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

import com.facebook.presto.orc.DataSink;
import com.facebook.presto.orc.stream.DataOutput;
import com.facebook.presto.spi.page.SerializedPage;
import io.airlift.units.DataSize;
import org.openjdk.jol.info.ClassLayout;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class PageWriter
        implements Closeable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PageWriter.class).instanceSize();

    private final DataSink dataSink;
    private long bufferedBytes;
    private long retainedBytes;
    private long maxBufferedBytes;
    private boolean closed;
    private List<DataOutput> bufferedPages;

    public PageWriter(DataSink dataSink, DataSize pageFileStripeMaxSize)
    {
        this.dataSink = requireNonNull(dataSink, "pageDataSink is null");
        this.maxBufferedBytes = requireNonNull(pageFileStripeMaxSize, "pageFileStripeMaxSize is null").toBytes();
        bufferedPages = new ArrayList<>();
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
            dataSink.write(bufferedPages);
            bufferedPages.clear();
            bufferedBytes = 0;
            retainedBytes = 0;
        }
        bufferedPages.add(pageDataOutput);
        bufferedBytes += writtenSize;
        retainedBytes += page.getRetainedSizeInBytes();
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        if (!bufferedPages.isEmpty()) {
            dataSink.write(bufferedPages);
        }
        dataSink.close();
    }

    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + retainedBytes + dataSink.getRetainedSizeInBytes();
    }
}
