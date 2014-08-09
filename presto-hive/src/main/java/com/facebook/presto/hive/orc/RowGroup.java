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

public class RowGroup
{
    private final int groupId;
    private final long rowCount;
    private final StreamSources streamSources;

    public RowGroup(int groupId, long rowCount, StreamSources streamSources)
    {
        this.groupId = groupId;
        this.rowCount = rowCount;
        this.streamSources = streamSources;
    }

    public int getGroupId()
    {
        return groupId;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public StreamSources getStreamSources()
    {
        return streamSources;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("groupId", groupId)
                .add("rowCount", rowCount)
                .add("streamSources", streamSources)
                .toString();
    }
}
