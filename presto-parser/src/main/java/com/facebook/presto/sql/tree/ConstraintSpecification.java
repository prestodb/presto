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

public final class ConstraintSpecification
        extends TableElement
{
    private final Optional<String> constraintName;
    private final List<String> constrainedColumns;
    private final Optional<ForeignKeyReferenceKey> foreignKeyReferenceKey;
    private final ConstraintType constraintType;
    private final boolean enabled;
    private final boolean rely;
    private final boolean enforced;

    public ConstraintSpecification(Optional<String> constraintName, List<String> constrainedColumns, ConstraintType constraintType, boolean enabled, boolean rely, boolean enforced)
    {
        this(constraintName, constrainedColumns, constraintType, enabled, rely, enforced, Optional.empty());
    }

    public ConstraintSpecification(Optional<String> constraintName, List<String> constrainedColumns, ConstraintType constraintType, boolean enabled, boolean rely, boolean enforced, Optional<ForeignKeyReferenceKey> foreignKeyReferenceKey)
    {
        this(Optional.empty(), constraintName, constrainedColumns, foreignKeyReferenceKey, constraintType, enabled, rely, enforced);
    }

    public ConstraintSpecification(NodeLocation location, Optional<String> constraintName, List<String> constrainedColumns, ConstraintType constraintType, boolean enabled, boolean rely, boolean enforced, Optional<ForeignKeyReferenceKey> foreignKeyReferenceKey)
    {
        this(Optional.of(location), constraintName, constrainedColumns, foreignKeyReferenceKey, constraintType, enabled, rely, enforced);
    }

    private ConstraintSpecification(Optional<NodeLocation> location, Optional<String> constraintName, List<String> constrainedColumns, Optional<ForeignKeyReferenceKey> foreignKeyReferenceKey, ConstraintType constraintType, boolean enabled, boolean rely, boolean enforced)
    {
        super(location);
        this.constraintName = requireNonNull(constraintName, "constraint name is null");
        this.constrainedColumns = requireNonNull(constrainedColumns, "constraint columns is null");
        // TODO : Add a check that referredColumns is not null for a foreign key constraint only
        this.foreignKeyReferenceKey = foreignKeyReferenceKey;
        this.constraintType = constraintType;
        this.enabled = enabled;
        this.rely = rely;
        this.enforced = enforced;
    }

    public Optional<String> getConstraintName()
    {
        return constraintName;
    }

    public List<String> getConstrainedColumns()
    {
        return constrainedColumns;
    }

    public Optional<ForeignKeyReferenceKey> getForeignKeyReferenceKey()
    {
        return foreignKeyReferenceKey;
    }

    public ConstraintType getConstraintType()
    {
        return constraintType;
    }

    public boolean isRely()
    {
        return rely;
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    public boolean isEnforced()
    {
        return enforced;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitConstraintSpecification(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ConstraintSpecification o = (ConstraintSpecification) obj;
        return Objects.equals(constraintName, o.constraintName) &&
                Objects.equals(constrainedColumns, o.constrainedColumns) &&
                Objects.equals(foreignKeyReferenceKey, o.foreignKeyReferenceKey) &&
                Objects.equals(constraintType, o.constraintType) &&
                Objects.equals(rely, o.rely) &&
                Objects.equals(enabled, o.enabled) &&
                Objects.equals(enforced, o.enforced);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(constraintName, constrainedColumns, foreignKeyReferenceKey, constraintType, rely, enabled, enforced);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("constraintName", constraintName)
                .add("constrainedColumns", constrainedColumns)
                .add("foreignKeyReferenceKey", foreignKeyReferenceKey)
                .add("constraintType", constraintType)
                .add("rely", rely)
                .add("enabled", enabled)
                .add("enforced", enforced)
                .toString();
    }

    public enum ConstraintType
    {
        UNIQUE,
        PRIMARY_KEY,
        FOREIGN_KEY
    }

    public static class ForeignKeyReferenceKey
    {
        QualifiedName tableName;
        List<String> columns;

        public ForeignKeyReferenceKey(QualifiedName tableName, List<String> columns)
        {
            this.tableName = tableName;
            this.columns = columns;
        }

        public QualifiedName getTableName()
        {
            return tableName;
        }

        public List<String> getColumns()
        {
            return columns;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ForeignKeyReferenceKey)) {
                return false;
            }
            ForeignKeyReferenceKey that = (ForeignKeyReferenceKey) o;
            return Objects.equals(tableName, that.tableName) && Objects.equals(columns, that.columns);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableName, columns);
        }

        @Override
        public String toString()
        {
            return "ForeignKeyReferenceKey{" +
                    "tableName=" + tableName +
                    ", columns=" + columns +
                    '}';
        }
    }
}
