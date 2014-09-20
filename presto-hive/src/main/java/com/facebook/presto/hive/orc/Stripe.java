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

import com.facebook.presto.hive.orc.stream.StreamSources;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class Stripe
{
    private final long rowCount;
    private final List<ColumnEncoding> columnEncodings;
    private final List<RowGroup> rowGroups;
    private final StreamSources dictionaryStreamSources;

    public Stripe(long rowCount, List<ColumnEncoding> columnEncodings, List<RowGroup> rowGroups, StreamSources dictionaryStreamSources)
    {
        this.rowCount = rowCount;
        this.columnEncodings = checkNotNull(columnEncodings, "columnEncodings is null");
        this.rowGroups = ImmutableList.copyOf(checkNotNull(rowGroups, "rowGroups is null"));
        this.dictionaryStreamSources = checkNotNull(dictionaryStreamSources, "dictionaryStreamSources is null");
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public List<ColumnEncoding> getColumnEncodings()
    {
        return columnEncodings;
    }

    public List<RowGroup> getRowGroups()
    {
        return rowGroups;
    }

    public StreamSources getDictionaryStreamSources()
    {
        return dictionaryStreamSources;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("rowCount", rowCount)
                .add("columnEncodings", columnEncodings)
                .add("rowGroups", rowGroups)
                .add("dictionaryStreams", dictionaryStreamSources)
                .toString();
    }
}
