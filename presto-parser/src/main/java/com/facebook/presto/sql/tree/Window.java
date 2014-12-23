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

import java.util.Objects;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class Window
        extends Node
{
    private final List<Expression> partitionBy;
    private final List<SortItem> orderBy;
    private final Optional<WindowFrame> frame;

    public Window(List<Expression> partitionBy, List<SortItem> orderBy, WindowFrame frame)
    {
        this.partitionBy = checkNotNull(partitionBy, "partitionBy is null");
        this.orderBy = checkNotNull(orderBy, "orderBy is null");
        this.frame = Optional.ofNullable(frame);
    }

    public List<Expression> getPartitionBy()
    {
        return partitionBy;
    }

    public List<SortItem> getOrderBy()
    {
        return orderBy;
    }

    public Optional<WindowFrame> getFrame()
    {
        return frame;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWindow(this, context);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Window o = (Window) obj;
        return Objects.equals(partitionBy, o.partitionBy) &&
                Objects.equals(orderBy, o.orderBy) &&
                Objects.equals(frame, o.frame);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionBy, orderBy, frame);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionBy", partitionBy)
                .add("orderBy", orderBy)
                .add("frame", frame)
                .toString();
    }
}
