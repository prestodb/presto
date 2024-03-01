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
package com.facebook.presto.memory;

import com.facebook.presto.spi.memory.MemoryPoolId;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class VersionedMemoryPoolId
{
    private final MemoryPoolId id;
    private final long version;

    public VersionedMemoryPoolId(MemoryPoolId id, long version)
    {
        this.id = requireNonNull(id, "id is null");
        this.version = version;
    }

    public MemoryPoolId getId()
    {
        return id;
    }

    public long getVersion()
    {
        return version;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("version", version)
                .toString();
    }
}
