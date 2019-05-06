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
package com.facebook.presto.orc.stream;

import com.facebook.presto.orc.OrcDataSourceId;
import io.airlift.slice.Slice;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MemoryOrcDataReader
        implements OrcDataReader
{
    private final OrcDataSourceId orcDataSourceId;
    private final Slice data;
    private final long retainedSize;

    public MemoryOrcDataReader(OrcDataSourceId orcDataSourceId, Slice data, long retainedSize)
    {
        this.orcDataSourceId = requireNonNull(orcDataSourceId, "orcDataSourceId is null");
        this.data = requireNonNull(data, "data is null");
        this.retainedSize = retainedSize;
    }

    @Override
    public OrcDataSourceId getOrcDataSourceId()
    {
        return orcDataSourceId;
    }

    @Override
    public long getRetainedSize()
    {
        return retainedSize;
    }

    @Override
    public int getSize()
    {
        return data.length();
    }

    @Override
    public int getMaxBufferSize()
    {
        return data.length();
    }

    @Override
    public Slice seekBuffer(int newPosition)
    {
        return data.slice(newPosition, data.length() - newPosition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("orcDataSourceId", orcDataSourceId)
                .add("dataSize", data.length())
                .toString();
    }
}
