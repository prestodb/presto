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
import static java.util.Objects.requireNonNull;

public class WindowSpecification
        extends Node
{
    private final Optional<Identifier> existingName;
    private final List<Expression> partitionBy;
    private final List<SortItem> orderBy;
    private final Optional<WindowFrame> frame;

    public WindowSpecification(Optional<Identifier> existingName, List<Expression> partitionBy, List<SortItem> orderBy, Optional<WindowFrame> frame)
    {
        this(Optional.empty(), existingName, partitionBy, orderBy, frame);
    }

    public WindowSpecification(NodeLocation location, Optional<Identifier> existingName, List<Expression> partitionBy, List<SortItem> orderBy, Optional<WindowFrame> frame)
    {
        this(Optional.of(location), existingName, partitionBy, orderBy, frame);
    }

    private WindowSpecification(Optional<NodeLocation> location, Optional<Identifier> existingName, List<Expression> partitionBy, List<SortItem> orderBy, Optional<WindowFrame> frame)
    {
        super(location);
        this.existingName = requireNonNull(existingName, "existingName is null");
        this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
        this.frame = requireNonNull(frame, "frame is null");
    }

    public Optional<Identifier> getExistingName()
    {
        return existingName;
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
        return visitor.visitWindowSpecification(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.addAll(partitionBy);
        nodes.addAll(orderBy);
        frame.ifPresent(nodes::add);
        return nodes.build();
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
        WindowSpecification that = (WindowSpecification) o;
        return Objects.equals(existingName, that.existingName) &&
                Objects.equals(partitionBy, that.partitionBy) &&
                Objects.equals(orderBy, that.orderBy) &&
                Objects.equals(frame, that.frame);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(existingName, partitionBy, orderBy, frame);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("existingName", existingName)
                .add("partitionBy", partitionBy)
                .add("orderBy", orderBy)
                .add("frame", frame)
                .toString();
    }
}
