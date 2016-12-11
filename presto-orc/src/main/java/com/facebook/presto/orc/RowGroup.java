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

import com.facebook.presto.orc.stream.InputStreamSources;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RowGroup
{
    private final int groupId;
    private final long rowOffset;
    private final long rowCount;
    private final InputStreamSources streamSources;

    public RowGroup(int groupId, long rowOffset, long rowCount, InputStreamSources streamSources)
    {
        this.groupId = groupId;
        this.rowOffset = rowOffset;
        this.rowCount = rowCount;
        this.streamSources = requireNonNull(streamSources, "streamSources is null");
    }

    public int getGroupId()
    {
        return groupId;
    }

    public long getRowOffset()
    {
        return rowOffset;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public InputStreamSources getStreamSources()
    {
        return streamSources;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("groupId", groupId)
                .add("rowOffset", rowOffset)
                .add("rowCount", rowCount)
                .add("streamSources", streamSources)
                .toString();
    }
}
