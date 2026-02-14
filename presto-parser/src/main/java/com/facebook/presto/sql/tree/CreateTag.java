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

public class CreateTag
        extends Statement
{
    private final QualifiedName tableName;
    private final boolean tableExists;
    private final boolean replace;
    private final boolean ifNotExists;
    private final String tagName;
    private final Optional<TableVersionExpression> tableVersion;
    private final Optional<Long> retainDays;

    public CreateTag(
            QualifiedName tableName,
            boolean tableExists,
            boolean replace,
            boolean ifNotExists,
            String tagName,
            Optional<TableVersionExpression> tableVersion,
            Optional<Long> retainDays)
    {
        this(Optional.empty(), tableName, tableExists, replace, ifNotExists, tagName, tableVersion, retainDays);
    }

    public CreateTag(
            NodeLocation location,
            QualifiedName tableName,
            boolean tableExists,
            boolean replace,
            boolean ifNotExists,
            String tagName,
            Optional<TableVersionExpression> tableVersion,
            Optional<Long> retainDays)
    {
        this(Optional.of(location), tableName, tableExists, replace, ifNotExists, tagName, tableVersion, retainDays);
    }

    private CreateTag(
            Optional<NodeLocation> location,
            QualifiedName tableName,
            boolean tableExists,
            boolean replace,
            boolean ifNotExists,
            String tagName,
            Optional<TableVersionExpression> tableVersion,
            Optional<Long> retainDays)
    {
        super(location);
        this.tableName = requireNonNull(tableName, "table is null");
        this.tableExists = tableExists;
        this.replace = replace;
        this.ifNotExists = ifNotExists;
        this.tagName = requireNonNull(tagName, "tagName is null");
        this.tableVersion = requireNonNull(tableVersion, "tableVersion is null");
        this.retainDays = requireNonNull(retainDays, "retainDays is null");
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public boolean isTableExists()
    {
        return tableExists;
    }

    public boolean isReplace()
    {
        return replace;
    }

    public boolean isIfNotExists()
    {
        return ifNotExists;
    }

    public String getTagName()
    {
        return tagName;
    }

    public Optional<TableVersionExpression> getTableVersion()
    {
        return tableVersion;
    }

    public Optional<Long> getRetainDays()
    {
        return retainDays;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateTag(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> children = ImmutableList.builder();
        tableVersion.ifPresent(children::add);
        return children.build();
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
        CreateTag that = (CreateTag) o;
        return tableExists == that.tableExists &&
                replace == that.replace &&
                ifNotExists == that.ifNotExists &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(tagName, that.tagName) &&
                Objects.equals(tableVersion, that.tableVersion) &&
                Objects.equals(retainDays, that.retainDays);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, tableExists, replace, ifNotExists, tagName, tableVersion, retainDays);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", tableName)
                .add("tableExists", tableExists)
                .add("replace", replace)
                .add("ifNotExists", ifNotExists)
                .add("tagName", tagName)
                .add("tableVersion", tableVersion)
                .add("retainDays", retainDays)
                .toString();
    }
}
