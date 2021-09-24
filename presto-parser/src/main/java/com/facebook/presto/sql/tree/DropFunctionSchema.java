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

public class DropFunctionSchema
        extends Statement
{
    private final boolean force;
    private final QualifiedName functionSchema;

    public DropFunctionSchema(QualifiedName functionSchema, boolean force)
    {
        this(Optional.empty(), functionSchema, force);
    }

    public DropFunctionSchema(NodeLocation location, QualifiedName functionSchema,
                              boolean force)
    {
        this(Optional.of(location), functionSchema, force);
    }

    public DropFunctionSchema(Optional<NodeLocation> location, QualifiedName functionSchema,
                              boolean force)
    {
        super(location);
        this.functionSchema = functionSchema;
        this.force = force;
    }

    public QualifiedName getFunctionSchema()
    {
        return functionSchema;
    }

    public boolean isForce()
    {
        return force;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.<Node>builder()
            .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(functionSchema);
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
        DropFunctionSchema o = (DropFunctionSchema) obj;
        return Objects.equals(functionSchema, o.functionSchema)
            && (force == o.force);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("functionSchema", functionSchema)
            .add("force", force)
            .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropFunctionSchema(this, context);
    }
}
