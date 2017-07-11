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
package com.facebook.presto.hive.parquet.memory;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LocalMemoryContext
{
    private final AbstractAggregatedMemoryContext parentMemoryContext;
    private long usedBytes;

    public LocalMemoryContext(AbstractAggregatedMemoryContext parentMemoryContext)
    {
        this.parentMemoryContext = requireNonNull(parentMemoryContext, "parentMemoryContext is null");
    }

    public long getBytes()
    {
        return usedBytes;
    }

    public void setBytes(long bytes)
    {
        long oldLocalUsedBytes = this.usedBytes;
        this.usedBytes = bytes;
        parentMemoryContext.updateBytes(this.usedBytes - oldLocalUsedBytes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("usedBytes", usedBytes)
                .toString();
    }
}
