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
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.orc.stream.StreamSource;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class StripeLayout
{
    private final long rowCount;
    private final List<ColumnEncoding> encodings;
    private final List<StreamLayout> dictionaryStreamLayouts;
    private final List<RowGroupLayout> rowGroupLayouts;

    public StripeLayout(long rowCount,
            List<ColumnEncoding> encodings,
            List<StreamLayout> dictionaryStreamLayouts,
            List<RowGroupLayout> rowGroupLayouts)
    {
        this.rowCount = rowCount;
        this.encodings = ImmutableList.copyOf(checkNotNull(encodings, "encodings is null"));
        this.dictionaryStreamLayouts = ImmutableList.copyOf(checkNotNull(dictionaryStreamLayouts, "dictionaryStreamLayouts is null"));
        this.rowGroupLayouts = ImmutableList.copyOf(checkNotNull(rowGroupLayouts, "rowGroupLayouts is null"));
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public List<ColumnEncoding> getEncodings()
    {
        return encodings;
    }

    public List<StreamLayout> getDictionaryStreamLayouts()
    {
        return dictionaryStreamLayouts;
    }

    public List<RowGroupLayout> getRowGroupLayouts()
    {
        return rowGroupLayouts;
    }

    public Stripe createStripe(List<StripeSlice> stripeSlices, int bufferSize)
    {
        // build the dictionary streams
        ImmutableMap.Builder<StreamId, StreamSource<?>> dictionaryStreamBuilder = ImmutableMap.builder();
        for (StreamLayout dictionaryStreamLayout : dictionaryStreamLayouts) {
            StreamSource<?> streamSource = dictionaryStreamLayout.createStreamSource(stripeSlices, bufferSize);
            dictionaryStreamBuilder.put(dictionaryStreamLayout.getStreamId(), streamSource);
        }
        StreamSources dictionaryStreamSources = new StreamSources(dictionaryStreamBuilder.build());

        // build the row groups
        ImmutableList.Builder<RowGroup> rowGroupBuilder = ImmutableList.builder();
        for (RowGroupLayout rowGroupLayout : rowGroupLayouts) {
            rowGroupBuilder.add(rowGroupLayout.createRowGroup(stripeSlices, bufferSize));
        }

        return new Stripe(rowCount, encodings, rowGroupBuilder.build(), dictionaryStreamSources);
    }
}
