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
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class DropSchema
        extends Statement
{
    private final QualifiedName schemaName;
    private final boolean exists;
    private final boolean cascade;

    public DropSchema(QualifiedName schemaName, boolean exists, boolean cascade)
    {
        this(Optional.empty(), schemaName, exists, cascade);
    }

    public DropSchema(NodeLocation location, QualifiedName schemaName, boolean exists, boolean cascade)
    {
        this(Optional.of(location), schemaName, exists, cascade);
    }

    private DropSchema(Optional<NodeLocation> location, QualifiedName schemaName, boolean exists, boolean cascade)
    {
        super(location);
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.exists = exists;
        this.cascade = cascade;
    }

    public QualifiedName getSchemaName()
    {
        return schemaName;
    }

    public boolean isExists()
    {
        return exists;
    }

    public boolean isCascade()
    {
        return cascade;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropSchema(this, context);
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
        DropSchema o = (DropSchema) obj;
        return Objects.equals(schemaName, o.schemaName) &&
                (exists == o.exists) &&
                (cascade == o.cascade);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, exists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("exists", exists)
                .add("cascade", cascade)
                .toString();
    }
}
