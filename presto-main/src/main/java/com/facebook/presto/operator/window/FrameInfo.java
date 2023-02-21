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
package com.facebook.presto.operator.window;

import com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType;
import com.facebook.presto.sql.planner.plan.WindowNode.Frame.WindowType;
import com.facebook.presto.sql.tree.SortItem.Ordering;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class FrameInfo
{
    private final WindowType type;
    private final BoundType startType;
    private final int startChannel;
    private final int sortKeyChannelForStartComparison;
    private final BoundType endType;
    private final int endChannel;
    private final int sortKeyChannelForEndComparison;
    private final int sortKeyChannel;
    private final Optional<Ordering> ordering;

    public FrameInfo(
            WindowType type,
            BoundType startType,
            Optional<Integer> startChannel,
            Optional<Integer> sortKeyChannelForStartComparison,
            BoundType endType,
            Optional<Integer> endChannel,
            Optional<Integer> sortKeyChannelForEndComparison,
            Optional<Integer> sortKeyChannel,
            Optional<Ordering> ordering)
    {
        this.type = requireNonNull(type, "type is null");
        this.startType = requireNonNull(startType, "startType is null");
        this.startChannel = requireNonNull(startChannel, "startChannel is null").orElse(-1);
        this.sortKeyChannelForStartComparison = requireNonNull(sortKeyChannelForStartComparison, "sortKeyChannelForStartComparison is null").orElse(-1);
        this.endType = requireNonNull(endType, "endType is null");
        this.endChannel = requireNonNull(endChannel, "endChannel is null").orElse(-1);
        this.sortKeyChannelForEndComparison = requireNonNull(sortKeyChannelForEndComparison, "sortKeyChannelForEndComparison is null").orElse(-1);
        this.sortKeyChannel = requireNonNull(sortKeyChannel, "sortKeyChannel is null").orElse(-1);
        this.ordering = requireNonNull(ordering, "ordering is null");
    }

    public WindowType getType()
    {
        return type;
    }

    public BoundType getStartType()
    {
        return startType;
    }

    public int getStartChannel()
    {
        return startChannel;
    }

    public int getSortKeyChannelForStartComparison()
    {
        return sortKeyChannelForStartComparison;
    }

    public BoundType getEndType()
    {
        return endType;
    }

    public int getEndChannel()
    {
        return endChannel;
    }

    public int getSortKeyChannelForEndComparison()
    {
        return sortKeyChannelForEndComparison;
    }

    public int getSortKeyChannel()
    {
        return sortKeyChannel;
    }

    public Optional<Ordering> getOrdering()
    {
        return ordering;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, startType, startChannel, sortKeyChannelForStartComparison, endType, endChannel, sortKeyChannelForEndComparison, sortKeyChannel, ordering);
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

        FrameInfo other = (FrameInfo) obj;

        return Objects.equals(this.type, other.type) &&
                Objects.equals(this.startType, other.startType) &&
                Objects.equals(this.sortKeyChannelForStartComparison, other.sortKeyChannelForStartComparison) &&
                Objects.equals(this.startChannel, other.startChannel) &&
                Objects.equals(this.endType, other.endType) &&
                Objects.equals(this.endChannel, other.endChannel) &&
                Objects.equals(this.sortKeyChannelForEndComparison, other.sortKeyChannelForEndComparison) &&
                Objects.equals(this.sortKeyChannel, other.sortKeyChannel) &&
                Objects.equals(this.ordering, other.ordering);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("startType", startType)
                .add("startChannel", startChannel)
                .add("sortKeyChannelForStartComparison", sortKeyChannelForStartComparison)
                .add("endType", endType)
                .add("endChannel", endChannel)
                .add("sortKeyChannelForEndComparison", sortKeyChannelForEndComparison)
                .add("sortKeyChannel", sortKeyChannel)
                .add("ordering", ordering)
                .toString();
    }
}
