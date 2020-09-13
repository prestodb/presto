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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class FetchFirst
        extends Node
{
    private final Optional<String> rowCount;
    private final boolean withTies;

    public FetchFirst(String rowCount)
    {
        this(Optional.empty(), Optional.of(rowCount), false);
    }

    public FetchFirst(String rowCount, boolean withTies)
    {
        this(Optional.empty(), Optional.of(rowCount), withTies);
    }

    public FetchFirst(Optional<String> rowCount)
    {
        this(Optional.empty(), rowCount, false);
    }

    public FetchFirst(Optional<String> rowCount, boolean withTies)
    {
        this(Optional.empty(), rowCount, withTies);
    }

    public FetchFirst(Optional<NodeLocation> location, Optional<String> rowCount, boolean withTies)
    {
        super(location);
        this.rowCount = rowCount;
        this.withTies = withTies;
    }

    public Optional<String> getRowCount()
    {
        return rowCount;
    }

    public boolean isWithTies()
    {
        return withTies;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitFetchFirst(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        FetchFirst that = (FetchFirst) o;
        return withTies == that.withTies &&
                Objects.equals(rowCount, that.rowCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rowCount, withTies);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("rowCount", rowCount.orElse(null))
                .add("withTies", withTies)
                .omitNullValues()
                .toString();
    }
}
