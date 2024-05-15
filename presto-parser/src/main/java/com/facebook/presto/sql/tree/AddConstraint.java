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

public final class AddConstraint
        extends Statement
{
    private final QualifiedName tableName;
    private final boolean tableExists;
    private final ConstraintSpecification constraintSpecification;

    public AddConstraint(QualifiedName tableName, boolean tableExists, ConstraintSpecification constraintSpecification)
    {
        this(Optional.empty(), tableName, tableExists, constraintSpecification);
    }

    public AddConstraint(NodeLocation location, QualifiedName tableName, boolean tableExists, ConstraintSpecification constraintSpecification)
    {
        this(Optional.of(location), tableName, tableExists, constraintSpecification);
    }

    private AddConstraint(Optional<NodeLocation> location, QualifiedName tableName, boolean tableExists, ConstraintSpecification constraintSpecification)
    {
        super(location);
        this.tableName = requireNonNull(tableName, "table name is null");
        this.tableExists = tableExists;
        this.constraintSpecification = constraintSpecification;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public boolean isTableExists()
    {
        return tableExists;
    }

    public ConstraintSpecification getConstraintSpecification()
    {
        return constraintSpecification;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAddConstraint(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, tableExists, constraintSpecification);
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
        AddConstraint o = (AddConstraint) obj;
        return Objects.equals(tableName, o.tableName) &&
                Objects.equals(tableExists, o.tableExists) &&
                Objects.equals(constraintSpecification, o.constraintSpecification);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("tableExists", tableExists)
                .add("constraintSpecification", constraintSpecification)
                .toString();
    }
}
