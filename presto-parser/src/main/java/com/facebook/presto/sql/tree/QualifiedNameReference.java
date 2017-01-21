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

public class QualifiedNameReference
        extends Expression
{
    private final QualifiedName name;

    public QualifiedNameReference(QualifiedName name)
    {
        this(Optional.empty(), name);
    }

    public QualifiedNameReference(NodeLocation location, QualifiedName name)
    {
        this(Optional.of(location), name);
    }

    private QualifiedNameReference(Optional<NodeLocation> location, QualifiedName name)
    {
        super(location);
        this.name = name;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public QualifiedName getSuffix()
    {
        return QualifiedName.of(name.getSuffix());
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitQualifiedNameReference(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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

        QualifiedNameReference that = (QualifiedNameReference) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }
}
