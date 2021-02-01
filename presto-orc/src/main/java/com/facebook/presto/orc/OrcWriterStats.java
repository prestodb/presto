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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.StripeInformation;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.orc.WriterStats.FlushReason.CLOSED;
import static com.facebook.presto.orc.WriterStats.FlushReason.DICTIONARY_FULL;
import static com.facebook.presto.orc.WriterStats.FlushReason.MAX_BYTES;
import static com.facebook.presto.orc.WriterStats.FlushReason.MAX_ROWS;
import static com.google.common.base.MoreObjects.toStringHelper;

public class OrcWriterStats
        implements WriterStats
{
    private final OrcWriterFlushStats allFlush = new OrcWriterFlushStats("ALL");
    private final OrcWriterFlushStats maxRowsFlush = new OrcWriterFlushStats(MAX_ROWS.name());
    private final OrcWriterFlushStats maxBytesFlush = new OrcWriterFlushStats(MAX_BYTES.name());
    private final OrcWriterFlushStats dictionaryFullFlush = new OrcWriterFlushStats(DICTIONARY_FULL.name());
    private final OrcWriterFlushStats closedFlush = new OrcWriterFlushStats(CLOSED.name());
    private final AtomicLong writerSizeInBytes = new AtomicLong();

    @Override
    public void recordStripeWritten(
            int stripeMinBytes,
            int stripeMaxBytes,
            int dictionaryMaxMemoryBytes,
            FlushReason flushReason,
            int dictionaryBytes,
            StripeInformation stripeInformation)
    {
        long stripeBytes = stripeInformation.getTotalLength();
        int stripeRows = stripeInformation.getNumberOfRows();
        getFlushStats(flushReason).recordStripeWritten(stripeBytes, stripeRows, dictionaryBytes);
        allFlush.recordStripeWritten(stripeBytes, stripeRows, dictionaryBytes);
    }

    @Override
    public void updateSizeInBytes(long deltaInBytes)
    {
        writerSizeInBytes.addAndGet(deltaInBytes);
    }

    @Managed
    @Nested
    public OrcWriterFlushStats getAllFlush()
    {
        return allFlush;
    }

    @Managed
    @Nested
    public OrcWriterFlushStats getMaxRowsFlush()
    {
        return maxRowsFlush;
    }

    @Managed
    @Nested
    public OrcWriterFlushStats getMaxBytesFlush()
    {
        return maxBytesFlush;
    }

    @Managed
    @Nested
    public OrcWriterFlushStats getDictionaryFullFlush()
    {
        return dictionaryFullFlush;
    }

    @Managed
    @Nested
    public OrcWriterFlushStats getClosedFlush()
    {
        return closedFlush;
    }

    @Managed
    public long getWriterSizeInBytes()
    {
        return writerSizeInBytes.get();
    }

    private OrcWriterFlushStats getFlushStats(FlushReason flushReason)
    {
        switch (flushReason) {
            case MAX_ROWS:
                return maxRowsFlush;
            case MAX_BYTES:
                return maxBytesFlush;
            case DICTIONARY_FULL:
                return dictionaryFullFlush;
            case CLOSED:
                return closedFlush;
            default:
                throw new IllegalArgumentException("unknown flush reason " + flushReason);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("allFlush", allFlush)
                .add("maxRowsFlush", maxRowsFlush)
                .add("maxBytesFlush", maxBytesFlush)
                .add("dictionaryFullFlush", dictionaryFullFlush)
                .add("closedFlush", closedFlush)
                .add("writerSizeInBytes", writerSizeInBytes.get())
                .toString();
    }
}
