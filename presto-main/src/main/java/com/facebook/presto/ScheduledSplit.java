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
package com.facebook.presto;

import com.facebook.presto.spi.Split;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.primitives.Longs;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduledSplit
{
    private final long sequenceId;
    private final Split split;

    @JsonCreator
    public ScheduledSplit(@JsonProperty("sequenceId") long sequenceId, @JsonProperty("split") Split split)
    {
        this.sequenceId = sequenceId;
        this.split = checkNotNull(split, "split is null");
    }

    @JsonProperty
    public long getSequenceId()
    {
        return sequenceId;
    }

    @JsonProperty
    public Split getSplit()
    {
        return split;
    }

    @Override
    public int hashCode()
    {
        return Longs.hashCode(sequenceId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final ScheduledSplit other = (ScheduledSplit) obj;
        return this.sequenceId == other.sequenceId;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("sequenceId", sequenceId)
                .add("split", split)
                .toString();
    }
}
