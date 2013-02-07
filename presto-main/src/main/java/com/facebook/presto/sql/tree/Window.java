package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import java.util.List;

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
        this.frame = Optional.fromNullable(frame);
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
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Window o = (Window) obj;
        return Objects.equal(partitionBy, o.partitionBy) &&
                Objects.equal(orderBy, o.orderBy) &&
                Objects.equal(frame, o.frame);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(partitionBy, orderBy, frame);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("partitionBy", partitionBy)
                .add("orderBy", orderBy)
                .add("frame", frame)
                .toString();
    }
}
