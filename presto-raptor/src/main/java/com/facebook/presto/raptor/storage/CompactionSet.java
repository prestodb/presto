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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.raptor.metadata.ShardMetadata;

import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CompactionSet
{
    private final long tableId;
    private final Set<ShardMetadata> shardsToCompact;

    public CompactionSet(long tableId, Set<ShardMetadata> shardsToCompact)
    {
        this.tableId = tableId;
        this.shardsToCompact = requireNonNull(shardsToCompact, "shardsToCompact is null");
    }

    public long getTableId()
    {
        return tableId;
    }

    public Set<ShardMetadata> getShardsToCompact()
    {
        return shardsToCompact;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionSet that = (CompactionSet) o;
        return Objects.equals(tableId, that.tableId) &&
                Objects.equals(shardsToCompact, that.shardsToCompact);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableId, shardsToCompact);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .add("shardsToCompact", shardsToCompact)
                .toString();
    }
}
