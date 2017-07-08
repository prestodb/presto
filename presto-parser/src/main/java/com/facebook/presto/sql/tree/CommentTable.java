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

public final class CommentTable
        extends Statement
{
    private final QualifiedName name;
    private final String comment;

    public CommentTable(QualifiedName name, String comment)
    {
        this(Optional.empty(), name, comment);
    }

    public CommentTable(NodeLocation location, QualifiedName name, String comment)
    {
        this(Optional.of(location), name, comment);
    }

    private CommentTable(Optional<NodeLocation> location, QualifiedName name, String comment)
    {
        super(location);
        this.name = requireNonNull(name, "table is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public String getComment()
    {
        return comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCommentTable(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, comment);
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
        CommentTable o = (CommentTable) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(comment, o.comment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("comment", comment)
                .toString();
    }
}
