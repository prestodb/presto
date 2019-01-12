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
package io.prestosql.plugin.raptor.legacy.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ColumnStats
{
    private final long columnId;
    private final Object min;
    private final Object max;

    @JsonCreator
    public ColumnStats(
            @JsonProperty("columnId") long columnId,
            @JsonProperty("min") @Nullable Object min,
            @JsonProperty("max") @Nullable Object max)
    {
        this.columnId = columnId;
        this.min = min;
        this.max = max;
    }

    @JsonProperty
    public long getColumnId()
    {
        return columnId;
    }

    @Nullable
    @JsonProperty
    public Object getMin()
    {
        return min;
    }

    @Nullable
    @JsonProperty
    public Object getMax()
    {
        return max;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnId", columnId)
                .add("min", min)
                .add("max", max)
                .omitNullValues()
                .toString();
    }
}
