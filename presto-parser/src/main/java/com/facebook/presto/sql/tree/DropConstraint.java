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

public class DropConstraint
        extends Statement
{
    private final QualifiedName tableName;
    private final String constraintName;
    private final boolean tableExists;
    private final boolean constraintExists;

    public DropConstraint(QualifiedName tableName, String constraintName, boolean tableExists, boolean constraintExists)
    {
        this(Optional.empty(), tableName, constraintName, tableExists, constraintExists);
    }

    public DropConstraint(NodeLocation location, QualifiedName tableName, String constraintName, boolean tableExists, boolean constraintExists)
    {
        this(Optional.of(location), tableName, constraintName, tableExists, constraintExists);
    }

    private DropConstraint(Optional<NodeLocation> location, QualifiedName tableName, String constraintName, boolean tableExists, boolean constraintExists)
    {
        super(location);
        this.tableName = requireNonNull(tableName, "table is null");
        this.constraintName = requireNonNull(constraintName, "constraint is null");
        this.tableExists = tableExists;
        this.constraintExists = constraintExists;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public String getConstraintName()
    {
        return constraintName;
    }

    public boolean isTableExists()
    {
        return tableExists;
    }

    public boolean isConstraintExists()
    {
        return constraintExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropConstraint(this, context);
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
        DropConstraint that = (DropConstraint) o;
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(constraintName, that.constraintName) &&
                Objects.equals(tableExists, that.tableExists) &&
                Objects.equals(constraintExists, constraintExists);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, tableExists, constraintName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", tableName)
                .add("constraint", constraintName)
                .add("tableExists", tableExists)
                .add("constraintExists", constraintExists)
                .toString();
    }
}
