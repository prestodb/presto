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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Window
        extends Node
{
    private final List<Expression> partitionBy;
    private final Optional<OrderBy> orderBy;
    private final Optional<WindowFrame> frame;

    public Window(List<Expression> partitionBy, Optional<OrderBy> orderBy, Optional<WindowFrame> frame)
    {
        this(Optional.empty(), partitionBy, orderBy, frame);
    }

    public Window(NodeLocation location, List<Expression> partitionBy, Optional<OrderBy> orderBy, Optional<WindowFrame> frame)
    {
        this(Optional.of(location), partitionBy, orderBy, frame);
    }

    private Window(Optional<NodeLocation> location, List<Expression> partitionBy, Optional<OrderBy> orderBy, Optional<WindowFrame> frame)
    {
        super(location);
        this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
        this.frame = requireNonNull(frame, "frame is null");
    }

    public List<Expression> getPartitionBy()
    {
        return partitionBy;
    }

    public Optional<OrderBy> getOrderBy()
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
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.addAll(partitionBy);
        orderBy.ifPresent(nodes::add);
        frame.ifPresent(nodes::add);
        return nodes.build();
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
