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

import com.facebook.presto.spi.analyzer.UpdateInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * AST node for:
 *   ALTER TABLE [IF EXISTS] tbl ADD COLUMN [IF NOT EXISTS] parent.field type [NOT NULL] [COMMENT '...']
 *
 * The column name is a dotted path: the last identifier is the new field name
 * and everything before it is the parent struct path.
 * Example: ALTER TABLE t ADD COLUMN col.sub_field VARCHAR
 *   -> columnPath = ["col"], fieldName = "sub_field"
 */
public class AddField
        extends Statement
{
    private final QualifiedName tableName;
    private final QualifiedName columnPath;   // parent struct path, e.g. ["col"] or ["col", "nested"]
    private final Identifier fieldName;       // the new field being added
    private final String type;
    private final boolean nullable;
    private final Optional<String> comment;
    private final boolean tableExists;
    private final boolean fieldNotExists;

    public AddField(
            NodeLocation location,
            QualifiedName tableName,
            QualifiedName columnPath,
            Identifier fieldName,
            String type,
            boolean nullable,
            Optional<String> comment,
            boolean tableExists,
            boolean fieldNotExists)
    {
        super(Optional.of(location));
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columnPath = requireNonNull(columnPath, "columnPath is null");
        this.fieldName = requireNonNull(fieldName, "fieldName is null");
        this.type = requireNonNull(type, "type is null");
        this.nullable = nullable;
        this.comment = requireNonNull(comment, "comment is null");
        this.tableExists = tableExists;
        this.fieldNotExists = fieldNotExists;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public QualifiedName getColumnPath()
    {
        return columnPath;
    }

    public Identifier getFieldName()
    {
        return fieldName;
    }

    public String getType()
    {
        return type;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    public boolean isTableExists()
    {
        return tableExists;
    }

    public boolean isFieldNotExists()
    {
        return fieldNotExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAddField(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public UpdateInfo getUpdateInfo()
    {
        return new UpdateInfo("ADD COLUMN", tableName.toString());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, columnPath, fieldName, type, nullable, comment, tableExists, fieldNotExists);
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
        AddField o = (AddField) obj;
        return Objects.equals(tableName, o.tableName) &&
                Objects.equals(columnPath, o.columnPath) &&
                Objects.equals(fieldName, o.fieldName) &&
                Objects.equals(type, o.type) &&
                nullable == o.nullable &&
                Objects.equals(comment, o.comment) &&
                tableExists == o.tableExists &&
                fieldNotExists == o.fieldNotExists;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("columnPath", columnPath)
                .add("fieldName", fieldName)
                .add("type", type)
                .add("nullable", nullable)
                .add("comment", comment)
                .add("tableExists", tableExists)
                .add("fieldNotExists", fieldNotExists)
                .toString();
    }
}
